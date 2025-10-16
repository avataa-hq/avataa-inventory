import copy
from datetime import timezone, datetime
from typing import Any, List, Union

from fastapi.encoders import jsonable_encoder
from google.protobuf.internal.well_known_types import Timestamp
from google.protobuf.json_format import MessageToDict
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import Null
from sqlmodel import select

from common.common_constant import AvailableInstances
from database import get_not_auth_session
from functions.functions_dicts import db_param_convert_by_val_type
from functions.functions_utils.utils import decode_multiple_value
from models import TPRM, Event
from routers.parameter_router.schemas import PRMReadMultiple
from services.event_service.constants import ProtoInstances
from services.listener_service.constants import PARAMETER_TYPE_INSTANCES_CACHE
from services.security_service.utils.get_user_data import (
    get_username_from_session,
)


class EventProcessor:
    def __init__(
        self,
        session: Session,
        key_class_name: str,
        key_event: str,
        data_to_send: Union[list, set],
    ):
        self._session = session
        self._key_class_name: str = key_class_name
        self._key_event: str = key_event
        self._data_to_send: Union[list, set] = copy.deepcopy(data_to_send)

        self._username = get_username_from_session(session=self._session)
        self._event_type = self._determine_event_type()

    def _process_object_type_event(
        self,
        object_type_instance: dict,
    ) -> None:
        new_session = next(get_not_auth_session())
        new_session.commit()

        object_type_instance["creation_date"] = (
            self._convert_datetime_from_timestamp(
                timestamp=object_type_instance["creation_date"]
            )
        )
        object_type_instance["modification_date"] = (
            self._convert_datetime_from_timestamp(
                timestamp=object_type_instance["modification_date"]
            )
        )

        event = Event(
            event={"TMO": jsonable_encoder(object_type_instance)},
            event_type=self._event_type,
            model_id=object_type_instance["id"],
            user=self._username,
        )
        new_session.add(event)
        new_session.commit()

    def _process_object_event(
        self,
        object_instance: dict,
    ) -> None:
        new_session = next(get_not_auth_session())
        new_session.commit()

        object_instance["creation_date"] = (
            self._convert_datetime_from_timestamp(
                timestamp=object_instance["creation_date"]
            )
        )
        object_instance["modification_date"] = (
            self._convert_datetime_from_timestamp(
                timestamp=object_instance["modification_date"]
            )
        )
        if object_instance.get("geometry"):
            object_instance["geometry"] = MessageToDict(
                object_instance["geometry"]
            )

        if object_instance.get("pov"):
            object_instance["pov"] = MessageToDict(object_instance["pov"])

        event = Event(
            event={
                "MO": jsonable_encoder(
                    object_instance,
                    custom_encoder={Null: lambda _: "null"},
                )
            },
            event_type=self._event_type,
            model_id=object_instance["id"],
            user=self._username,
        )

        new_session.add(event)
        new_session.commit()

    def _process_parameter_type_event(
        self,
        param_type_instance: dict,
    ) -> None:
        new_session = next(get_not_auth_session())
        new_session.commit()

        if self._event_type == "TPRMDelete":
            PARAMETER_TYPE_INSTANCES_CACHE[param_type_instance["id"]] = (
                param_type_instance
            )

        param_type_instance["creation_date"] = (
            self._convert_datetime_from_timestamp(
                timestamp=param_type_instance["creation_date"]
            )
        )
        param_type_instance["modification_date"] = (
            self._convert_datetime_from_timestamp(
                timestamp=param_type_instance["modification_date"]
            )
        )
        event = Event(
            event={"TPRM": jsonable_encoder(param_type_instance)},
            event_type=self._event_type,
            model_id=param_type_instance["id"],
            user=self._username,
        )
        new_session.add(event)
        new_session.commit()

    def _process_parameter_event(
        self,
        parameter_instance: dict,
    ) -> None:
        new_session = next(get_not_auth_session())
        new_session.commit()

        param_type_instance = PARAMETER_TYPE_INSTANCES_CACHE.get(
            parameter_instance["tprm_id"]
        )

        if not param_type_instance:
            query = select(TPRM).where(TPRM.id == parameter_instance["tprm_id"])
            param_type_instance = new_session.execute(query).scalar()
            param_type_instance = dict(param_type_instance)

        if param_type_instance["multiple"]:
            param_to_read = PRMReadMultiple(
                id=parameter_instance["id"],
                tprm_id=parameter_instance["tprm_id"],
                mo_id=parameter_instance["mo_id"],
                value=decode_multiple_value(parameter_instance["value"]),
                version=parameter_instance["version"],
            )

        else:
            param_to_read = db_param_convert_by_val_type[
                param_type_instance["val_type"]
            ](
                parameter_instance["id"],
                parameter_instance["tprm_id"],
                parameter_instance["mo_id"],
                parameter_instance["value"],
                parameter_instance["version"],
            )

        event = Event(
            event={"PRM": jsonable_encoder(param_to_read.dict())},
            event_type=self._event_type,
            model_id=parameter_instance["id"],
            user=self._username,
        )

        new_session.add(event)
        new_session.commit()

    def _determine_event_type(self):
        new_key_event = {
            "created": "Create",
            "updated": "Update",
            "deleted": "Delete",
        }

        return str(self._key_class_name) + new_key_event[self._key_event]

    @staticmethod
    def _convert_datetime_from_timestamp(timestamp: Timestamp) -> str:
        seconds = timestamp.seconds
        nanos = timestamp.nanos
        dt = datetime.fromtimestamp(
            timestamp=seconds + nanos / 1e9, tz=timezone.utc
        )
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    def execute(self):
        for new_object in self._data_to_send:
            match self._key_class_name:
                case "TMO":
                    self._process_object_type_event(
                        object_type_instance=new_object
                    )

                case "MO":
                    self._process_object_event(object_instance=new_object)

                case "TPRM":
                    self._process_parameter_type_event(
                        param_type_instance=new_object
                    )

                case "PRM":
                    self._process_parameter_event(parameter_instance=new_object)


class ConvertInstancesToProto:
    def __init__(self, instance_type: str, data_to_convert: list[Any]):
        self._instance_type = instance_type
        self._data_to_convert = data_to_convert

    @staticmethod
    def _convert_dict_instances_to_proto_format(
        instances: List[dict], instance_type: ProtoInstances
    ):
        for index, instance in enumerate(instances):
            instances[index] = instance_type.value(**instance)

        return instances

    def convert(self):
        match self._instance_type:
            case AvailableInstances.TMO.value:
                return self._convert_dict_instances_to_proto_format(
                    instances=self._data_to_convert,
                    instance_type=ProtoInstances.TMO,
                )

            case AvailableInstances.MO.value:
                return self._convert_dict_instances_to_proto_format(
                    instances=self._data_to_convert,
                    instance_type=ProtoInstances.MO,
                )

            case AvailableInstances.TPRM.value:
                return self._convert_dict_instances_to_proto_format(
                    instances=self._data_to_convert,
                    instance_type=ProtoInstances.TPRM,
                )

            case AvailableInstances.PRM.value:
                return self._convert_dict_instances_to_proto_format(
                    instances=self._data_to_convert,
                    instance_type=ProtoInstances.PRM,
                )
