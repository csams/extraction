from functools import partial, reduce

from insights.core import dr
from insights.core.archives import extract
from insights.core.hydration import create_context
from insights.core.plugins import is_datasource
from insights.combiners.hostname import hostname
from insights.combiners.redhat_release import redhat_release
from insights.specs import Specs

from extraction.specs import is_large


def compose(*args):
    return lambda x: reduce(lambda r, f: f(r), reversed(args), x)


def liftI(f):
    def inner(x):
        return (f(i) for i in x)
    return inner


def to_dict(line):
    return {"content": line}


def meta(**kwargs):
    def inner(data):
        data.update(kwargs)
        return data
    return inner


def make_counter():
    c = [0]

    def inner(data):
        data["number"] = c[0]
        c[0] += 1
        return data
    return inner


def file_reader(f):
    yield f.read()


def line_reader(f):
    return f


def get_spec(spec, broker):
    return broker[spec].content[0] if spec in broker else ""


get_release = partial(get_spec, Specs.redhat_release)
get_uname = partial(get_spec, Specs.uname)


def get_hostname(broker):
    hn = broker.get(hostname)
    return hn.fqdn if hn else ""


def get_version(broker):
    rel = broker.get(redhat_release)
    return [str(rel.major), str(rel.minor)] if rel else ["-1", "-1"]


def create_broker(path):
    ctx = create_context(path)
    broker = dr.Broker()
    broker[ctx.__class__] = ctx
    return broker


def get_datasources():
    all_datasources = set()
    for n in dir(Specs):
        a = getattr(Specs, n)
        if is_datasource(a):
            all_datasources.add(a)
    return all_datasources


class ExtractionContext(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def process_dir(self, path):
        broker = create_broker(path)
        broker = dr.run(broker=broker)

        archive_meta = meta(hostname=get_hostname(broker),
                            uname=get_uname(broker),
                            release=get_release(broker),
                            version=get_version(broker),
                            **self.kwargs)

        datasources = get_datasources() & set(broker.instances)
        for d in datasources:
            name = dr.get_simple_name(d)
            large = is_large(name)
            reader = line_reader if large else file_reader

            providers = broker[d]
            if not isinstance(providers, list):
                providers = [providers]

            for p in providers:
                file_meta = meta(path=p.path, target=name)
                transformer = compose(archive_meta, file_meta)
                if large:
                    transformer = compose(transformer, make_counter())
                stream_transformer = liftI(compose(transformer, to_dict))
                yield (name, p.path, compose(stream_transformer, reader))

    def process(self, path):
        with extract(path) as ext:
            for item in self.process_dir(ext.tmp_dir):
                yield item
