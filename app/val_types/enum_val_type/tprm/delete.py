from sqlalchemy import select
from sqlalchemy.orm import Session

from models import TPRM, TMO
from val_types.enum_val_type.exceptions import NotFoundParameterType


class EnumTPRMDeleter:
    def __init__(
        self,
        session: Session,
        param_types_to_delete: list[TPRM],
        object_type_instance: TMO,
        autocommit: bool = True,
    ):
        self.session = session
        self.param_types_to_delete = param_types_to_delete
        self.object_type_instance = object_type_instance
        self.autocommit = autocommit

    def _validate_requested_tprms_for_existing(self):
        requested_tprm_names = {
            param_type.name for param_type in self.param_types_to_delete
        }

        stmt = select(TPRM.name).where(
            TPRM.name.in_(requested_tprm_names),
            TPRM.tmo_id == self.object_type_instance.id,
        )
        exists_tprm_names = set(self.session.execute(stmt).scalars().all())

        not_exists_tprms = requested_tprm_names.difference(exists_tprm_names)
        if not_exists_tprms:
            raise NotFoundParameterType(
                status_code=422,
                detail=f"There are TPRMs in request, that are not exists: {not_exists_tprms}",
            )

    def delete_enum_tprms(self) -> tuple[list[str], list[TPRM]]:
        errors: list[str] = []
        tprms_copy = [
            tprm.copy(deep=True) for tprm in self.param_types_to_delete
        ]
        for tprm in self.param_types_to_delete:
            self.session.delete(tprm)

        self.session.flush()
        if self.autocommit:
            self.session.commit()
        return errors, tprms_copy
