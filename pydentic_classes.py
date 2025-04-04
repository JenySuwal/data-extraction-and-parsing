from pydantic import BaseModel, Field
from typing import List, Optional, Union

class GenericPartsData(BaseModel):
    thread_size: Optional[List[str]] = Field([], description="measurement value")
    material_surface: Optional[List[str]] = Field([], description="Material composition and surface treatments")