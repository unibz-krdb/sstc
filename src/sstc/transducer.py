from .generator import Generator
from .transducer_context import TransducerContext


class Transducer:
    def __init__(self, ctx: TransducerContext):
        self.ctx = ctx

    @classmethod
    def from_file(
        cls, source_schema_path: str, source_constraints_path: str, target_path: str
    ):
        return cls(
            TransducerContext.from_files(
                universal_path=source_schema_path,
                source_path=source_constraints_path,
                target_path=target_path,
            )
        )

    def compile(self) -> str:
        return Generator(self.ctx).compile()
