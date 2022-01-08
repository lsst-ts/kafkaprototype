from typing import List

from pydantic import BaseModel, Field, conlist


class Model(BaseModel):
    int_5: List[int] = Field(
        default_factory=lambda: [0, 0, 0, 0, 0],
        description="5 ints",
        min_items=5,
        max_items=5,
        units="unitless",
        sal_type="long",
    )
    bool_5: conlist(bool, min_items=5, max_items=5) = [False] * 5


m = Model()
m.bool_5 = [5]
print("m=", m)
