import pytest
from src.processing.validator import require_fields
from src.common.exceptions import ValidationError


def test_require_fields_missing():
    with pytest.raises(ValidationError):
        require_fields({"a": 1}, ["a", "b"])
