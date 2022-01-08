from typing import List

from pydantic import create_model, Field, conlist


Model = create_model(
    "Model",
    int_5=(conlist(int, min_items=5, max_items=5), [0] * 5),
    bool_5=(
        List[bool],
        Field(default_factory=lambda: [False] * 5, min_items=5, max_items=5),
    ),
    string=(
        str,
        Field("", max_length=20),
    ),
    int=(int, 0),
)


m = Model()  # int_5=[0, 1, 2, 3])
print("m =", m)
