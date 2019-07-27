// automatically generated by the FlatBuffers compiler, do not modify


#ifndef FLATBUFFERS_GENERATED_FBMETA_TABLES_H_
#define FLATBUFFERS_GENERATED_FBMETA_TABLES_H_

#include "flatbuffers/flatbuffers.h"

namespace Tables {

struct FB_Meta;

struct FB_Meta FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_FORMAT_TYPE = 4,
    VT_IS_DELETED = 6,
    VT_DATA_SIZE = 8,
    VT_GLOBAL_OFF = 10,
    VT_LEN = 12,
    VT_COMPRESSION_TYPE = 14,
    VT_DATA = 16
  };
  int32_t format_type() const {
    return GetField<int32_t>(VT_FORMAT_TYPE, 0);
  }
  bool is_deleted() const {
    return GetField<uint8_t>(VT_IS_DELETED, 0) != 0;
  }
  uint64_t data_size() const {
    return GetField<uint64_t>(VT_DATA_SIZE, 0);
  }
  uint64_t global_off() const {
    return GetField<uint64_t>(VT_GLOBAL_OFF, 0);
  }
  uint64_t len() const {
    return GetField<uint64_t>(VT_LEN, 0);
  }
  int32_t compression_type() const {
    return GetField<int32_t>(VT_COMPRESSION_TYPE, 0);
  }
  const flatbuffers::Vector<uint8_t> *data() const {
    return GetPointer<const flatbuffers::Vector<uint8_t> *>(VT_DATA);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<int32_t>(verifier, VT_FORMAT_TYPE) &&
           VerifyField<uint8_t>(verifier, VT_IS_DELETED) &&
           VerifyField<uint64_t>(verifier, VT_DATA_SIZE) &&
           VerifyField<uint64_t>(verifier, VT_GLOBAL_OFF) &&
           VerifyField<uint64_t>(verifier, VT_LEN) &&
           VerifyField<int32_t>(verifier, VT_COMPRESSION_TYPE) &&
           VerifyOffset(verifier, VT_DATA) &&
           verifier.VerifyVector(data()) &&
           verifier.EndTable();
  }
};

struct FB_MetaBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_format_type(int32_t format_type) {
    fbb_.AddElement<int32_t>(FB_Meta::VT_FORMAT_TYPE, format_type, 0);
  }
  void add_is_deleted(bool is_deleted) {
    fbb_.AddElement<uint8_t>(FB_Meta::VT_IS_DELETED, static_cast<uint8_t>(is_deleted), 0);
  }
  void add_data_size(uint64_t data_size) {
    fbb_.AddElement<uint64_t>(FB_Meta::VT_DATA_SIZE, data_size, 0);
  }
  void add_global_off(uint64_t global_off) {
    fbb_.AddElement<uint64_t>(FB_Meta::VT_GLOBAL_OFF, global_off, 0);
  }
  void add_len(uint64_t len) {
    fbb_.AddElement<uint64_t>(FB_Meta::VT_LEN, len, 0);
  }
  void add_compression_type(int32_t compression_type) {
    fbb_.AddElement<int32_t>(FB_Meta::VT_COMPRESSION_TYPE, compression_type, 0);
  }
  void add_data(flatbuffers::Offset<flatbuffers::Vector<uint8_t>> data) {
    fbb_.AddOffset(FB_Meta::VT_DATA, data);
  }
  explicit FB_MetaBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  FB_MetaBuilder &operator=(const FB_MetaBuilder &);
  flatbuffers::Offset<FB_Meta> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<FB_Meta>(end);
    return o;
  }
};

inline flatbuffers::Offset<FB_Meta> CreateFB_Meta(
    flatbuffers::FlatBufferBuilder &_fbb,
    int32_t format_type = 0,
    bool is_deleted = false,
    uint64_t data_size = 0,
    uint64_t global_off = 0,
    uint64_t len = 0,
    int32_t compression_type = 0,
    flatbuffers::Offset<flatbuffers::Vector<uint8_t>> data = 0) {
  FB_MetaBuilder builder_(_fbb);
  builder_.add_len(len);
  builder_.add_global_off(global_off);
  builder_.add_data_size(data_size);
  builder_.add_data(data);
  builder_.add_compression_type(compression_type);
  builder_.add_format_type(format_type);
  builder_.add_is_deleted(is_deleted);
  return builder_.Finish();
}

inline flatbuffers::Offset<FB_Meta> CreateFB_MetaDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    int32_t format_type = 0,
    bool is_deleted = false,
    uint64_t data_size = 0,
    uint64_t global_off = 0,
    uint64_t len = 0,
    int32_t compression_type = 0,
    const std::vector<uint8_t> *data = nullptr) {
  auto data__ = data ? _fbb.CreateVector<uint8_t>(*data) : 0;
  return Tables::CreateFB_Meta(
      _fbb,
      format_type,
      is_deleted,
      data_size,
      global_off,
      len,
      compression_type,
      data__);
}

inline const Tables::FB_Meta *GetFB_Meta(const void *buf) {
  return flatbuffers::GetRoot<Tables::FB_Meta>(buf);
}

inline const Tables::FB_Meta *GetSizePrefixedFB_Meta(const void *buf) {
  return flatbuffers::GetSizePrefixedRoot<Tables::FB_Meta>(buf);
}

inline bool VerifyFB_MetaBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifyBuffer<Tables::FB_Meta>(nullptr);
}

inline bool VerifySizePrefixedFB_MetaBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifySizePrefixedBuffer<Tables::FB_Meta>(nullptr);
}

inline void FinishFB_MetaBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<Tables::FB_Meta> root) {
  fbb.Finish(root);
}

inline void FinishSizePrefixedFB_MetaBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<Tables::FB_Meta> root) {
  fbb.FinishSizePrefixed(root);
}

}  // namespace Tables

#endif  // FLATBUFFERS_GENERATED_FBMETA_TABLES_H_