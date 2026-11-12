"""读者句柄：取得一个release后，后续查询只按该映射读取。

句柄在打开时一次性固化release ID与表映射，之后不再查看current指针；
关闭句柄不删除任何文件（本次不做GC）。
"""
from .errors import UnknownTable


class ReleaseHandle:
    def __init__(self, catalog, release_id, mapping):
        self._catalog = catalog
        self.release_id = release_id
        self.mapping = dict(mapping)  # table -> snapshot_id，已固化
        self._closed = False

    def _ensure_open(self):
        if self._closed:
            raise UnknownTable("句柄已关闭")

    def tables(self):
        self._ensure_open()
        return sorted(self.mapping)

    def snapshot_id(self, table):
        self._ensure_open()
        try:
            return self.mapping[table]
        except KeyError:
            raise UnknownTable("release %s 中无表 %s" % (self.release_id, table))

    def manifest(self, table):
        return self._catalog.snapshot_manifest(self.snapshot_id(table))

    def rows(self, table, verify=True):
        """按固化映射读取表内容；不为每张表重新看current。"""
        self._ensure_open()
        manifest = self.manifest(table)
        rows = []
        for f in manifest["files"]:
            rows.extend(self._catalog.store.read_rows(f, verify=verify))
        return rows

    def close(self):
        """关闭只释放句柄本身，不删除任何文件。"""
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False
