from functools import partial

from insights.core import dr
from insights.core.archives import extract
from insights.core.hydration import create_context
from insights.core.plugins import is_datasource
from insights.combiners.hostname import hostname
from insights.combiners.redhat_release import redhat_release
from insights.specs import Specs

from extraction.specs import is_large

dr.load_components("insights.specs.default")
dr.load_components("insights.specs.sos_archive")


all_datasources = set()
for n in dir(Specs):
    a = getattr(Specs, n)
    if is_datasource(a):
        all_datasources.add(a)


def get_hostname(broker):
    hn = broker.get(hostname)
    if hn:
        return hn.fqdn


def get_release(broker):
    rel = broker.get(redhat_release)
    if rel:
        return [rel.major, rel.minor]
    return [-1, -1]


def get_uname(broker):
    if Specs.uname in broker:
        return broker[Specs.uname].content[0]


def add_host_meta(hn, rel, uname, data):
    for d in data:
        d["hostname"] = hn
        d["release"] = rel
        d["uname"] = uname
        yield d


def create_broker(path):
    ctx = create_context(path)
    broker = dr.Broker()
    broker[ctx.__class__] = ctx
    return broker


def file_reader(f):
    yield f.read()


def line_reader(f):
    return f


class RecordGenerator(object):
    def __init__(self, it):
        self.it = it

    def enhance(self, data):
        return {"content": data}

    def __iter__(self):
        for content in self.it:
            yield self.enhance(content)


class SosRecordGenerator(RecordGenerator):
    def __init__(self, it, **kwargs):
        super(SosRecordGenerator, self).__init__(it)
        self.dct = kwargs

    def enhance(self, data):
        dct = self.dct.copy()
        dct["content"] = data
        return dct


class LargeRecordGenerator(SosRecordGenerator):
    def __init__(self, *args, **kwargs):
        super(LargeRecordGenerator, self).__init__(*args, **kwargs)
        self.index = 0

    def enhance(self, data):
        dct = super(LargeRecordGenerator, self).enhance(data)
        dct["number"] = self.index
        self.index += 1
        return dct


class ExtractionContext(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def get_generator(self, large=False):
        Generator = LargeRecordGenerator if large else SosRecordGenerator
        return partial(Generator, **self.kwargs)

    def get_reader(self, large=False):
        return line_reader if large else file_reader

    def process_file(self, gen, reader, path):
        # delay the open so we just have a stack of generators and no open
        # file handles until we start reading the stream
        yield None
        with open(path) as f:
            for l in gen(reader(f)):
                yield l

    def process_files(self, gen, reader, paths):
        process_file = partial(self.process_file, gen, reader)
        return (ent for p in paths for ent in process_file(p) if ent)

    def process_provider(self, gen, reader, provider):
        for ent in self.process_file(gen, reader, provider.path):
            if ent:
                ent["path"] = provider.relative_path
                yield ent

    def process_providers(self, files, large=False):
        Gen = self.get_generator(large)
        reader = self.get_reader(large)
        process_provider = partial(self.process_provider, Gen, reader)
        return (ent for provider in files for ent in process_provider(provider))

    def process_spec(self, spec, providers):
        name = dr.get_simple_name(spec)
        for ent in self.process_providers(providers, is_large(name)):
            ent["target"] = dr.get_simple_name(spec)
            yield ent

    def process(self, path):
        with extract(path) as ext:
            broker = create_broker(ext.tmp_dir)
            broker = dr.run(broker=broker)

            hn = get_hostname(broker)
            release = get_release(broker)
            uname = get_uname(broker)

            add_meta = partial(add_host_meta, hn, release, uname)

            datasources = all_datasources & set(broker.instances)
            for d in datasources:
                providers = broker[d]
                if not isinstance(providers, list):
                    providers = [providers]
                stream = add_meta(self.process_spec(d, providers))
                yield (d, stream)
