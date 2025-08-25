from typing import *
import polars as pl

class Config:
    default_cse = ["chrom", "start", "end"]
    default_locus = "_locus_"

    @classmethod
    @property
    def locus(cls) -> pl.Expr:
        return pl.col(cls.default_locus)

@pl.api.register_dataframe_namespace("loc")
class LocusDataFrameNamespace:
    def __init__(self, df):
        self._df = df
    
    def init(self, suffix: str | None = None) -> pl.DataFrame:
        if suffix:
            from_cols = [f"chrom{suffix}", f"start{suffix}", f"end{suffix}"]
        else:
            from_cols = Config.default_cse
        to_cols = Config.default_cse

        return self._df.with_columns(
            pl.struct([
                pl.col(from_col).alias(to_col)
                for from_col, to_col in zip(from_cols, to_cols)
            ]).alias(Config.default_locus)
        )
    
    def exit(self, suffix: str | None = "", unnest: str | None = None, 
        unnest_to: List[str] | Literal["default"] = "default") -> pl.DataFrame:
        """Unnest the struct to columns in unnest_to maintaining column order
        
        On collision, replaces original columns in unnest_to with the unnested columns
        """
        df = self._df
        unnest = unnest or Config.default_locus
        unnest_to = Config.default_cse if unnest_to == "default" else unnest_to
        unnest_to = [f"{col}{suffix}" for col in unnest_to]
        changing = [unnest, *unnest_to]
        rest_cols = [col for col in df.columns if col not in changing]
        final_cols = [col for col in df.columns if col != unnest]

        unnested = (
            df.select(unnest)
            .unnest(unnest)
            .rename({k: v for k, v in zip(Config.default_cse, unnest_to)})
        )
        rest = df.select(rest_cols)
        final = pl.concat([unnested, rest], how="horizontal").select(*final_cols)
        return final

    def resize(self, size: int, how: Literal["center", "start", "end"] = "center", suffix: str = "") -> pl.DataFrame:
        """Resize start and end position"""
        return (
            self._df.loc.init(suffix)
            .with_columns(Config.locus.loc.resize(size, how).alias(Config.default_locus))
            .loc.exit(suffix)
        )

@pl.api.register_expr_namespace("loc")
class LocusExpressionNamespace:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

   
    @property
    def fields(self) -> pl.Expr:
        """Get struct field names"""
        return (self._expr.struct.field(col) for col in Config.default_cse)

    def struct(self, chrom, start, end) -> pl.Expr:
        """Get new struct"""
        return pl.struct([
            col.alias(default) 
            for col, default 
            in zip([chrom, start, end], Config.default_cse)
        ])

    def target(self) -> pl.Expr:
        """Get struct column to perform operations on"""
        return self._expr.col(Config.default_locus)

    def resize(self, size: int, how: Literal["center", "start", "end"] = "center") -> pl.Expr:
        """Resize start and end position"""
        chrom, start, end = self.fields

        if how == "center":
            center = (start + end) // 2
            start = center - size // 2
            end = start + size
        elif how == "start":
            start = end - size
        elif how == "end":
            end = start + size

        return self.struct(chrom, start, end)

    

df = pl.DataFrame({
    "chrom1": ["chr1", "chr2", "chrX"],
    "start1": [10, 1000, 150],
    "end1": [20, 1280, 230]
})

print(df.loc.resize(10, suffix="1")) 
