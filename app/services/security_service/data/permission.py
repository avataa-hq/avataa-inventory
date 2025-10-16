from collections import namedtuple

from models import MO, TMO
from services.security_service.data.permissions.inventory import (
    MOPermission,
    TMOPermission,
)

Permission = namedtuple("Permission", ["main", "security", "column"])

db_permissions = {
    MO.__tablename__: [
        Permission(main=MO, security=MOPermission, column="id"),
        Permission(main=MO, security=TMOPermission, column="tmo_id"),
    ],
    TMO.__tablename__: Permission(
        main=TMO, security=TMOPermission, column="id"
    ),
}

db_admins = {"realm_access.__admin"}
