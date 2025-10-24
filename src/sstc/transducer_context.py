from .source_context import SourceContext
from .target_context import TargetContext


class TransducerContext:
    source: SourceContext
    target: TargetContext

    def __init__(self, source: SourceContext, target: TargetContext):
        self.source = source
        self.target = target

    @classmethod
    def from_files(
        cls, source_schema_path: str, source_constraints_path: str, target_path: str
    ):
        source_context = SourceContext.from_file(
            schema_path=source_schema_path, constraints_path=source_constraints_path
        )
        target_context = TargetContext.from_file(
            target_path, source_context=source_context
        )
        return cls(source=source_context, target=target_context)
