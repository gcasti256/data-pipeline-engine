"""Tests for pipeline_engine.transforms.map — column mapping and expression evaluation."""

from __future__ import annotations

from pipeline_engine.transforms.map import MapTransform

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMapTransform:
    def test_computed_column(self):
        """A new column computed from an expression is added correctly."""
        mt = MapTransform(columns={"total": "price * quantity"})
        data = [
            {"price": 10, "quantity": 5},
            {"price": 20, "quantity": 3},
        ]
        result = mt.execute(data)

        assert len(result) == 2
        assert result[0]["total"] == 50
        assert result[1]["total"] == 60
        # Original columns are preserved by default
        assert result[0]["price"] == 10

    def test_type_cast(self):
        """int() and float() casts in expressions work correctly."""
        mt = MapTransform(columns={"age_int": "int(age_str)"})
        data = [
            {"name": "Alice", "age_str": "30"},
            {"name": "Bob", "age_str": "25"},
        ]
        result = mt.execute(data)

        assert result[0]["age_int"] == 30
        assert result[1]["age_int"] == 25
        assert isinstance(result[0]["age_int"], int)

    def test_string_functions(self):
        """String functions like upper() and lower() work."""
        mt = MapTransform(columns={"name_upper": "upper(name)"})
        data = [{"name": "alice"}, {"name": "Bob"}]
        result = mt.execute(data)

        assert result[0]["name_upper"] == "ALICE"
        assert result[1]["name_upper"] == "BOB"

    def test_simple_rename(self):
        """A plain column name as the expression acts as a rename/copy."""
        mt = MapTransform(columns={"full_name": "name"})
        data = [{"name": "Alice", "age": 30}]
        result = mt.execute(data)

        assert result[0]["full_name"] == "Alice"
        assert result[0]["name"] == "Alice"  # original preserved

    def test_drop_original_columns(self):
        """When drop_original=True, only mapped columns appear in the output."""
        mt = MapTransform(
            columns={"n": "name", "a": "int(age)"},
            drop_original=True,
        )
        data = [{"name": "Alice", "age": "30", "city": "NYC"}]
        result = mt.execute(data)

        assert set(result[0].keys()) == {"n", "a"}
        assert result[0]["n"] == "Alice"
        assert result[0]["a"] == 30

    def test_string_concatenation(self):
        """String concatenation via + operator."""
        mt = MapTransform(
            columns={"greeting": "name + ' is ' + str(age)"},
        )
        data = [{"name": "Alice", "age": 30}]
        result = mt.execute(data)
        assert result[0]["greeting"] == "Alice is 30"

    def test_arithmetic_expression(self):
        """Multi-step arithmetic expressions evaluate correctly."""
        mt = MapTransform(columns={"result": "a + b * 2"})
        data = [{"a": 10, "b": 5}]
        result = mt.execute(data)
        assert result[0]["result"] == 20  # 10 + (5 * 2)
