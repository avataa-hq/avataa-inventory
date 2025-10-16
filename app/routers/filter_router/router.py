from fastapi import APIRouter, HTTPException

from routers.object_router.utils import dict_of_filter_flags

router = APIRouter(tags=["Filter helper"], prefix="/query_params_filters_info")


@router.get("", status_code=200)
async def read_common_info():
    result = """If some of routers able to use query params to obtain filtered results, you can add
     additional params to
     your request. \n
    Example : \n
    ?tprm_id8|contains=value&tprm_id122|is_any_of=1;2;3&filter_logical_operator=or \n \n
    Where: \n
    - tprm_id*number* : id of TPRM \n
    - contains, is_any_of: filter flags. Able filter flags depends of TPRM type
     [str, date, datetime, float, int, bool, mo_link, prm_link, user_link, formula] \n
    - value : search value \n
    - 1;2;3 : search values with separator ;  - use if flag able to search by several values \n
    - filter_logical_operator: logical operator can be one of: and, or. By default equals 'and' \n
    """
    return result


@router.get("/flags_by_tprm_type/{tprm_type}")
async def read_available_filter_flags_for_particular_tprm_type(tprm_type: str):
    if tprm_type in dict_of_filter_flags:
        return dict_of_filter_flags.get(tprm_type)
    return HTTPException(
        status_code=404,
        detail=f"TPRM type - {tprm_type} does not implement. "
        f"Implemented TPRM types: {list(dict_of_filter_flags)}",
    )
