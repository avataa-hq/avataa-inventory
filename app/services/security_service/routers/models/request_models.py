from pydantic import BaseModel, Field


class CreatePermission(BaseModel):
    parent_id: int = Field(..., ge=1, alias="itemId")
    permission: str = Field(
        ...,
        description="merged resolution from keycloak. Example: realm_access.__admin",
        min_length=1,
    )
    create: bool = Field(..., description="can create objects of this type")
    read: bool = Field(..., description="can read objects of this type")
    update: bool = Field(..., description="can update objects of this type")
    delete: bool = Field(..., description="can delete objects of this type")
    admin: bool = Field(
        ..., description="can administrate objects of this type"
    )

    def get_actions(self) -> dict[str, bool]:
        return {
            "create": self.create,
            "read": self.read,
            "update": self.update,
            "delete": self.delete,
            "admin": self.admin,
        }

    class Config:
        populate_by_name = True


class CreatePermissions(CreatePermission):
    permission: list[str] = Field(..., min_items=1)


class UpdatePermission(BaseModel):
    create: bool | None = Field(None)
    read: bool | None = Field(None)
    update: bool | None = Field(None)
    delete: bool | None = Field(None)
    admin: bool | None = Field(None)

    def get_actions(self, exclude_unset: bool = False) -> dict[str, bool]:
        if not exclude_unset:
            return {
                "create": self.create,
                "read": self.read,
                "update": self.update,
                "delete": self.delete,
                "admin": self.admin,
            }
        result = {}
        if self.create is not None:
            result["create"] = self.create
        if self.read is not None:
            result["read"] = self.read
        if self.update is not None:
            result["update"] = self.update
        if self.delete is not None:
            result["delete"] = self.delete
        if self.admin is not None:
            result["admin"] = self.admin
        return result

    class Config:
        populate_by_name = True
