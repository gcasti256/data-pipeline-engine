from __future__ import annotations

import ast
import operator
import re
from collections.abc import Callable
from typing import Any

from pipeline_engine.transforms.base import BaseTransform

# ---------------------------------------------------------------------------
# Safe expression evaluator
# ---------------------------------------------------------------------------

_BINARY_OPS: dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}

_UNARY_OPS: dict[type, Callable[[Any], Any]] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}

_SAFE_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "int": int,
    "float": float,
    "str": str,
    "upper": lambda s: str(s).upper(),
    "lower": lambda s: str(s).lower(),
    "strip": lambda s: str(s).strip(),
    "round": round,
    "len": len,
    "abs": abs,
}

# Matches a function call like ``int(age_str)`` or ``round(price, 2)``.
_FUNC_CALL_RE = re.compile(r"^(\w+)\((.+)\)$", re.DOTALL)


class _ExpressionEvaluator:
    """Evaluate a simple expression against a record dict.

    Supported syntax:
    * Column references: bare identifiers (``price``, ``quantity``)
    * Numeric and string literals
    * Binary arithmetic: ``+``, ``-``, ``*``, ``/``
    * String concatenation via ``+``
    * Function calls: ``int(x)``, ``float(x)``, ``str(x)``, ``upper(x)``,
      ``lower(x)``, ``strip(x)``, ``round(x)``/``round(x, n)``, ``len(x)``,
      ``abs(x)``
    """

    def __init__(self, expr: str, record: dict[str, Any]) -> None:
        self._record = record
        self._expr = expr

    def evaluate(self) -> Any:
        tree = ast.parse(self._expr, mode="eval")
        return self._eval_node(tree.body)

    # -- Node dispatch -------------------------------------------------------

    def _eval_node(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            return node.value

        if isinstance(node, ast.Name):
            return self._resolve_name(node.id)

        if isinstance(node, ast.BinOp):
            return self._eval_binop(node)

        if isinstance(node, ast.UnaryOp):
            return self._eval_unaryop(node)

        if isinstance(node, ast.Call):
            return self._eval_call(node)

        # Fallback: support JoinedStr (f-strings) is out of scope.
        raise ValueError(
            f"Unsupported expression node: {type(node).__name__}"
        )

    # -- Helpers -------------------------------------------------------------

    def _resolve_name(self, name: str) -> Any:
        if name in self._record:
            return self._record[name]
        # Allow safe function names to be referenced for higher-order use
        if name in _SAFE_FUNCTIONS:
            return _SAFE_FUNCTIONS[name]
        raise KeyError(f"Column {name!r} not found in record")

    def _eval_binop(self, node: ast.BinOp) -> Any:
        left = self._eval_node(node.left)
        right = self._eval_node(node.right)
        op_fn = _BINARY_OPS.get(type(node.op))
        if op_fn is None:
            raise ValueError(
                f"Unsupported binary operator: {type(node.op).__name__}"
            )
        return op_fn(left, right)

    def _eval_unaryop(self, node: ast.UnaryOp) -> Any:
        operand = self._eval_node(node.operand)
        op_fn = _UNARY_OPS.get(type(node.op))
        if op_fn is None:
            raise ValueError(
                f"Unsupported unary operator: {type(node.op).__name__}"
            )
        return op_fn(operand)

    def _eval_call(self, node: ast.Call) -> Any:
        if not isinstance(node.func, ast.Name):
            raise ValueError("Only simple function calls are supported")
        func_name = node.func.id
        if func_name not in _SAFE_FUNCTIONS:
            raise ValueError(f"Function {func_name!r} is not allowed")
        func = _SAFE_FUNCTIONS[func_name]
        args = [self._eval_node(arg) for arg in node.args]
        return func(*args)


def _safe_eval(expr: str, record: dict[str, Any]) -> Any:
    """Evaluate *expr* against *record* without using ``eval()``."""
    return _ExpressionEvaluator(expr, record).evaluate()


# ---------------------------------------------------------------------------
# MapTransform
# ---------------------------------------------------------------------------

class MapTransform(BaseTransform):
    """Transform columns by renaming, casting, or computing new values.

    Parameters
    ----------
    columns:
        Mapping of ``{output_column: expression}``.  The expression is either:

        * A bare column name for a simple rename (``{"new": "old"}``).
        * A safe expression string:
          ``{"total": "price * quantity"}``,
          ``{"age": "int(age_str)"}``,
          ``{"full_name": "first_name + ' ' + last_name"}``.
    drop_original:
        When *True*, columns from the source record that are **not** listed
        as output columns are dropped from the result.
    """

    def __init__(
        self,
        columns: dict[str, str],
        drop_original: bool = False,
        name: str = "",
    ) -> None:
        super().__init__(name=name)
        self._columns = columns
        self._drop_original = drop_original

    def execute(
        self,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.validate_input(data)
        results: list[dict[str, Any]] = []
        for record in data:
            new_record = self._transform_record(record)
            results.append(new_record)
        return results

    def _transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        if self._drop_original:
            out: dict[str, Any] = {}
        else:
            out = dict(record)

        for out_col, expr in self._columns.items():
            # Simple rename: expression is just an existing column name.
            if expr in record and not _looks_like_expression(expr):
                out[out_col] = record[expr]
            else:
                out[out_col] = _safe_eval(expr, record)

        return out


def _looks_like_expression(expr: str) -> bool:
    """Return *True* if *expr* contains operators or calls (i.e. is not a
    plain identifier)."""
    return bool(
        set(expr) & {"+", "-", "*", "/", "(", ")", " "}
    )
