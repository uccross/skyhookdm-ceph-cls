# C/C++ Language Server Setup

The `Dockerfile.langserver` file defines an image with dependencies for a 
terminal-based C/C++ IDE (clangd, neovim+languageclient). To build the image:

```bash
cd path/to/skyhookdm-ceph-cls/

docker build -t builder docker/builder-langserver
```

And to start developing inside the container:

```bash
docker run --rm -ti \
  -v $PWD:/workspace \
  -w /workspace \
  builder
```

> **NOTE**: To avoid issues with file permissions on Linux, use 
> [`podman`](https://podmain.io) instead.

The above puts us inside the container, where we can run `cmake` and 
[`ninja`](https://ninja-build.org/) as we would normally do:

```bash
cmake \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -G Ninja \
  -B build/ \
  -S .

ninja -C build/
```

## C/C++ Language Server

Now, what about the IDE features? We first need to link to the JSON file 
containing the commands that CMake generated:

```bash
ln -s build/compile_commands.json .
```

This file contains the commands to build the project and pass all the 
right flags to the compiler. This file can then be consumed by 
[`clangd`](https://clangd.llvm.org) in order to provide [language 
server](https://langserver.org) capabilities.

## (neo) VIM

For the editor, the image contains [Neovim](https://neovim.io/), as 
well as an [opinionated `.vimrc`](./vimrc) file properly setup to work 
with `clangd`. In particular, the [coc.nvim][coc] plugin is used, 
along with its [coc-clangd][coc-clangd] extension, and a list of 
[recommended settings][recommended].

To test this, inside the container, run `vim`. Once in Vim, install 
the plugins by typing `:PlugInstall` and then install the language 
server plugin by installing `:CocInstall coc-clangd`. Close and reopen 
Vim and open a C++ file; then type any keyword in order to get 
autocomplete suggestions, or get markers on the left for errors, or a 
floating box describing the issue:

<img width="600" src="https://user-images.githubusercontent.com/473117/90477641-9bd5d600-e0e0-11ea-8c89-a52f0aa41857.png">

## Editor (outside container)

To interact with `clangd` from outside the container (e.g. from 
another editor in your host machine), we can expose it on a network 
port:

```bash
docker run --rm -ti \
  -p 50505:50505 \
  -v $PWD:$PWD \
  -w $PWD \
  builder
```

On the container prompt, we can start `clangd` and redirect stdio with 
[`socat`](https://github.com/autozimu/LanguageClient-neovim/issues/741#issuecomment-466355883):

```bash
socat tcp-listen:50505,reuseaddr exec:clangd
```

And finally point the editor plugin to `tcp://127.0.0.1:50505`. For 
example, in the case of [LanguageClient-neovim][lgc], that would look 
like:

```vimscript
let g:LanguageClient_serverCommands = {
 \ 'cpp': ['tcp://127.0.0.1:50505'],
 \ 'c': ['tcp://127.0.0.1:50505'],
 \ }
```

Note that some things won't be available in the above "outside of the 
container" setup, most notably definitions within files that are 
inside the container cannot be reached by the editor. Also, the path 
in the host has to be the same path within the container as `clangd` 
expects this.

[coc]: https://github.com/neoclide/coc.nvim/
[coc-clangd]: https://github.com/clangd/coc-clangd/
[lgc]: https://github.com/autozimu/LanguageClient-neovim
[recommended]: https://github.com/neoclide/coc.nvim#example-vim-configuration
