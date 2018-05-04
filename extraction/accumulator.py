import json
import os

from insights import dr
from insights.util import ensure_dir, KeyPassingDefaultDict
from extraction.io import File
from extraction.specs import is_large


KB = 1024 ** 1
MB = 1024 ** 2
GB = 1024 ** 3


class Writer(object):
    def __init__(self, root, name, max_size):
        self.root = root
        self.name = name
        self.max_size = max_size
        self.files = []
        self.cur_file = None

        self.ext = ".json."
        self.index = 0

        path = self.make_file_path()
        while os.path.exists(path):
            self.files.append(File(path))
            self.index += 1
            path = self.make_file_path()

        if self.index:
            self.index -= 1

        if not self.files:
            self.files.append(File(path))

        if len(self.files[-1]) >= self.max_size:
            self.next_file()

        self.cur_size = self.get_cur_size()

    def get_cur_size(self):
        return len(self.files[-1])

    def make_file_path(self):
        idx = str(self.index).zfill(5)
        return os.path.join(self.root, self.name + self.ext + idx)

    def next_file(self):
        self.index += 1
        path = self.make_file_path()
        self.files.append(File(path))

    def should_roll(self, data):
        return self.cur_size >= self.max_size

    def write_stream(self, stream):
        self.cur_size = self.get_cur_size()
        for s in stream:
            self.write(s)

    def write(self, data):
        if self.should_roll(data):
            self.close()
            self.next_file()

        if not self.cur_file:
            self.cur_file = self.files[-1].open(mode="a")

        self.cur_size += self.cur_file.write(json.dumps(data) + "\n")

    def close(self):
        if self.cur_file:
            self.cur_file.flush()
            self.cur_file.close()
            self.cur_file = None
            self.cur_size = 0

    def __iadd__(self, other):
        for f in other.files:
            if (len(self.files[-1]) >= self.max_size or
               (self.cur_size != 0 and self.cur_size + len(f) >= self.max_size)):
                self.next_file()
            self.files[-1] += f
        self.cur_size = len(self.files[-1])
        return self


class LargeWriter(Writer):
    def should_roll(self, data):
        return (super(LargeWriter, self).should_roll(data) and
                data.get("number") == 0)


class Accumulator(object):
    def __init__(self, root, small_max=200 * MB, large_max=1 * GB):

        def make_writer(d):
            n = dr.get_simple_name(d)
            if is_large(n):
                return LargeWriter(root, n, large_max)
            return Writer(root, n, small_max)

        def create_writers():
            def partition():
                small, large = set(), set()
                for f in os.listdir(root):
                    name = f.split(".", 1)[0]
                    (large if is_large(name) else small).add(name)
                return small, large

            small, large = partition()
            results = {s: Writer(root, s, small_max) for s in small}
            results.update({l: LargeWriter(root, l, large_max) for l in large})
            return results

        ensure_dir(root)

        self.root = root
        self.small_max = small_max
        self.large_max = large_max

        self.writers = KeyPassingDefaultDict(make_writer)
        self.writers.update(create_writers())

    def _handle_stream(self, k, stream):
        name = dr.get_simple_name(k)
        writer = self.writers[name]
        writer.write_stream(stream)
        writer.close()
        return writer

    def process(self, ctx, archive):
        for k, stream in ctx.process(archive):
            self._handle_stream(k, stream)

    def __iadd__(self, other):
        for k, them in other.writers.items():
            self.writers[k] += them
        return self
