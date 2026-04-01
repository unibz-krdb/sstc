from .context import Context


class TransducerContext:
    source: Context
    target: Context

    def __init__(self, source: Context, target: Context):
        self.source = source
        self.target = target

    @classmethod
    def from_files(cls, universal_path: str, source_path: str, target_path: str):
        source_context = Context.from_file(
            universal_path=universal_path,
            context_path=source_path,
            direction="source",
        )
        target_context = Context.from_file(
            universal_path=universal_path,
            context_path=target_path,
            direction="target",
        )
        return cls(source=source_context, target=target_context)
