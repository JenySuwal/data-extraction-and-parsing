from pydantic import BaseModel, Field
from typing import List, Optional, Union

class GenericPartsData(BaseModel):
    seal_type: Optional[List[str]] = Field(default=[],description="Type of seal used in the bearing. Common examples include 'shielded', 'unshielded', and 'open'.")
    material_surface: Optional[List[str]] = Field([], description="Material composition and surface treatments")


class MisumiPartNumber(BaseModel):
    part_number:str=Field(...,description="partnumber of the mechinal parts")