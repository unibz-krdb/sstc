from .source_context import SourceContext
from .target_context import TargetContext


class Context:
    source: SourceContext
    target: TargetContext

    def __init__(self, source: SourceContext, target: TargetContext):
        self.source = source
        self.target = target

    @classmethod
    def from_files(cls, source_file_path: str, target_file_path: str):
        source_context = SourceContext.from_file(source_file_path)
        target_context = TargetContext.from_file(
            target_file_path, source_context=source_context
        )
        return cls(source=source_context, target=target_context)
