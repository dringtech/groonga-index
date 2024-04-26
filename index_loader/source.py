import pyarrow.parquet as pq
from adlfs import AzureBlobFileSystem


class SourceFile(object):
    def __init__(self, parquet_path):
        self.path = parquet_path
        self._fs = None
        if 'abfs' in self.path:
            self._fs = AzureBlobFileSystem()
        self._data = pq.ParquetFile(self.path, filesystem=self._fs)

    @property
    def num_row_groups(self):
        return self._data.num_row_groups

    @property
    def column_names(self):
        return self._data.schema.names

    def read_row_groups(self, *args, **kwargs):
        return self._data.read_row_groups(*args, **kwargs)
  
    def download(self, target):
        if self._fs is not None:
            self._fs.download(self.path, target)
