import json
from collections import namedtuple
from collections.abc import MutableMapping

from graphql import (
    ExecutionResult,
    GraphQLError,
    execute,
    get_operation_ast,
    parse,
    validate,
)
from graphql import format_error as format_error_default

from .error import HttpQueryError

# Necessary only for static type checking
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from graphql import GraphQLSchema  # noqa: F401


class SkipException(Exception):
    pass


GraphQLParams = namedtuple("GraphQLParams", "query,variables,operation_name")
GraphQLResponse = namedtuple("GraphQLResponse", "result,status_code")


def run_http_query(
    schema: "GraphQLSchema",
    request_method: str,
    data: Union[Dict, List[Dict]],
    query_data: Optional[Dict] = None,
    batch_enabled: bool = False,
    catch: bool = False,
    **execute_options: Dict
):
    if request_method not in ("get", "post"):
        raise HttpQueryError(
            405,
            "GraphQL only supports GET and POST requests.",
            headers={"Allow": "GET, POST"},
        )
    if catch:
        catch_exc: Union[Type[HttpQueryError], Type[SkipException]] = HttpQueryError
    else:
        catch_exc = SkipException
    is_batch = isinstance(data, list)

    is_get_request = request_method == "get"
    allow_only_query = is_get_request

    if not is_batch:
        if not isinstance(data, (dict, MutableMapping)):
            raise HttpQueryError(
                400, "GraphQL params should be a dict. Received {}.".format(data)
            )
        data = [data]
    elif not batch_enabled:
        raise HttpQueryError(400, "Batch GraphQL requests are not enabled.")

    if not data:
        raise HttpQueryError(400, "Received an empty list in the batch request.")

    extra_data: Dict[str, Any] = {}
    # If is a batch request, we don't consume the data from the query
    if not is_batch:
        extra_data = query_data or {}

    all_params = [get_graphql_params(entry, extra_data) for entry in data]

    responses = [
        get_response(schema, params, catch_exc, allow_only_query, **execute_options)
        for params in all_params
    ]

    return responses, all_params


def encode_execution_results(
    execution_results: List[Optional[ExecutionResult]],
    format_error: Callable[[Exception], Dict],
    is_batch: bool,
    encode: Callable[[Dict], Any],
) -> Tuple[Any, int]:
    responses = [
        format_execution_result(execution_result, format_error)
        for execution_result in execution_results
    ]
    result, status_codes = zip(*responses)
    status_code = max(status_codes)

    if not is_batch:
        result = result[0]

    return encode(result), status_code


def json_encode(data: Dict, pretty: bool = False) -> str:
    if not pretty:
        return json.dumps(data, separators=(",", ":"))

    return json.dumps(data, indent=2, separators=(",", ": "))


def load_json_variables(variables: Optional[Union[str, Dict]]) -> Optional[Dict]:
    if variables and isinstance(variables, str):
        try:
            return json.loads(variables)
        except Exception:
            raise HttpQueryError(400, "Variables are invalid JSON.")
    return variables  # type: ignore


def get_graphql_params(data: Dict, query_data: Dict) -> GraphQLParams:
    query = data.get("query") or query_data.get("query")
    variables = data.get("variables") or query_data.get("variables")
    # document_id = data.get('documentId')
    operation_name = data.get("operationName") or query_data.get("operationName")

    return GraphQLParams(query, load_json_variables(variables), operation_name)


def get_response(
    schema: "GraphQLSchema",
    params: GraphQLParams,
    catch: Type[BaseException],
    allow_only_query: bool = False,
    **kwargs: Dict
) -> Optional[ExecutionResult]:
    try:
        execution_result = execute_graphql_request(schema, params, allow_only_query)
    except catch:
        return None

    return execution_result


def format_execution_result(
    execution_result: Optional[ExecutionResult],
    format_error: Optional[Callable[[Exception], Dict]] = None,
) -> GraphQLResponse:
    status_code = 200

    response: Optional[Dict[str, Any]]
    if execution_result:
        if execution_result.errors:
            if not format_error:
                format_errors = format_error_default
            response = {"errors": [format_errors(e) for e in execution_result.errors]}
        else:
            response = {"data": execution_result.data}
    else:
        response = None

    return GraphQLResponse(response, status_code)


def execute_graphql_request(
    schema: "GraphQLSchema", params: GraphQLParams, allow_only_query: bool = False
):
    if not params.query:
        raise HttpQueryError(400, "Must provide query string.")

    try:
        document = parse(params.query)
    except GraphQLError as e:
        return ExecutionResult(data=None, errors=[e])
    except Exception as e:
        e = GraphQLError(str(e), original_error=e)
        return ExecutionResult(data=None, errors=[e])

    if allow_only_query:
        operation_ast = get_operation_ast(document, params.operation_name)
        if operation_ast and operation_ast.operation.value != "query":
            raise HttpQueryError(
                405,
                "Can only perform a {} operation from a POST request.".format(
                    operation_ast.operation.value
                ),
                headers={"Allow": "POST"},
            )

    # Note: the schema is not validated here for performance reasons.
    # This should be done only once when starting the server.

    validation_errors = validate(schema, document)
    if validation_errors:
        return ExecutionResult(data=None, errors=validation_errors)

    return execute(
        schema,
        document,
        variable_values=params.variables,
        operation_name=params.operation_name,
    )


def load_json_body(data: str) -> Dict:
    try:
        return json.loads(data)
    except Exception:
        raise HttpQueryError(400, "POST body sent invalid JSON.")


__all__ = [
    "GraphQLParams",
    "HttpQueryError",
    "SkipException",
    "run_http_query",
    "encode_execution_results",
    "json_encode",
    "load_json_variables",
    "get_graphql_params",
    "get_response",
    "format_execution_result",
    "execute_graphql_request",
    "load_json_body",
]
