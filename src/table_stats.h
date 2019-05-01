namespace diverdb {

class TableStats {
 public:
  TableStats() : num_valid_entries_(0), num_delete_entries_(0) {}

  ~TableStats() {}

  // Accessors.
  size_t num_valid_entries() const { return num_valid_entries_; }
  size_t num_delete_entries() const { return num_delete_entries_; }

 protected:
  // Accessors.
  size_t& mutable_num_valid_entries() { return num_valid_entries_; }
  size_t& mutable_num_delete_entries() { return num_delete_entries_; }

 private:
  // The number of entries that are not deletes.
  size_t num_valid_entries_;

  // The number of entries that are deletes.
  size_t num_delete_entries_;
};

}  // namespace diverdb
