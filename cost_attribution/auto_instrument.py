"""
Cost Attribution - Auto-Instrumentation.

Patches boto3 and LLM SDKs (openai, anthropic) so SDK calls can be captured
without explicit add_api_call() calls in each function.
"""

from __future__ import annotations

import functools
import threading

_patched = False
_patch_lock = threading.Lock()


def auto_instrument(
    boto3: bool = True,
    openai: bool = True,
    anthropic: bool = True,
) -> None:
    """Patch configured SDKs once per process."""
    global _patched
    with _patch_lock:
        if _patched:
            return
        if boto3:
            _patch_boto3()
        if openai:
            _patch_openai()
        if anthropic:
            _patch_anthropic()
        _patched = True


def is_instrumented() -> bool:
    """Return True once auto_instrument() has executed."""
    return _patched


def _record_call(service_key: str, count: int = 1, input_tokens: int = 0, output_tokens: int = 0) -> None:
    try:
        from .core.context import add_api_call

        kwargs: dict = {"count": count}
        if input_tokens:
            kwargs["input_tokens"] = input_tokens
        if output_tokens:
            kwargs["output_tokens"] = output_tokens
        add_api_call(service_key, **kwargs)
    except Exception:
        pass


_BOTO3_OP_MAP: dict[tuple[str, str], str] = {
    ("dynamodb", "GetItem"): "dynamodb_read",
    ("dynamodb", "Query"): "dynamodb_query",
    ("dynamodb", "Scan"): "dynamodb_read",
    ("dynamodb", "PutItem"): "dynamodb_write",
    ("dynamodb", "UpdateItem"): "dynamodb_write",
    ("dynamodb", "DeleteItem"): "dynamodb_write",
    ("dynamodb", "BatchGetItem"): "dynamodb_read",
    ("dynamodb", "BatchWriteItem"): "dynamodb_write",
    ("dynamodb", "TransactGetItems"): "dynamodb_read",
    ("dynamodb", "TransactWriteItems"): "dynamodb_write",
    ("s3", "GetObject"): "s3_get",
    ("s3", "HeadObject"): "s3_get",
    ("s3", "PutObject"): "s3_put",
    ("s3", "CopyObject"): "s3_put",
    ("s3", "DeleteObject"): "s3_put",
    ("s3", "ListObjectsV2"): "s3_list",
    ("s3", "ListObjects"): "s3_list",
    ("sqs", "SendMessage"): "sqs_send",
    ("sqs", "SendMessageBatch"): "sqs_send",
    ("sqs", "ReceiveMessage"): "sqs_receive",
    ("sqs", "DeleteMessage"): "sqs_receive",
    ("sns", "Publish"): "sns_publish",
    ("lambda", "InvokeFunction"): "aws_lambda_request",
    ("lambda", "Invoke"): "aws_lambda_request",
    ("apigateway", "TestInvokeMethod"): "api_gateway_request",
    ("bedrock-runtime", "InvokeModel"): "bedrock_claude_3_sonnet",
    ("bedrock-runtime", "InvokeModelWithResponseStream"): "bedrock_claude_3_sonnet",
}


def _patch_boto3() -> None:
    try:
        import botocore.session  # type: ignore

        session = botocore.session.get_session()
        session.register("before-call", _boto3_before_call_handler)
    except Exception:
        pass

    try:
        import botocore.client  # type: ignore

        original_call = botocore.client.BaseClient._make_api_call  # type: ignore

        @functools.wraps(original_call)
        def patched_make_api_call(self, operation_name, api_params):
            service_name = self.meta.service_model.service_name
            key = (service_name, operation_name)
            service_key = _BOTO3_OP_MAP.get(key)
            result = original_call(self, operation_name, api_params)

            if service_key:
                input_tokens = 0
                output_tokens = 0
                if service_name == "bedrock-runtime":
                    try:
                        body = result.get("body")
                        if hasattr(body, "read"):
                            import io
                            import json

                            raw = body.read()
                            result["body"] = io.BytesIO(raw)
                            parsed = json.loads(raw)
                            usage = parsed.get("usage") or {}
                            input_tokens = int(usage.get("input_tokens", 0))
                            output_tokens = int(usage.get("output_tokens", 0))

                            model_id = api_params.get("modelId", "")
                            if "haiku" in model_id:
                                service_key = "bedrock_claude_3_haiku"
                            elif "opus" in model_id:
                                service_key = "bedrock_claude_3_opus"
                            else:
                                service_key = "bedrock_claude_3_sonnet"
                    except Exception:
                        pass
                _record_call(service_key, count=1, input_tokens=input_tokens, output_tokens=output_tokens)
            return result

        botocore.client.BaseClient._make_api_call = patched_make_api_call  # type: ignore
    except Exception:
        pass


def _boto3_before_call_handler(event_name, **kwargs):
    del kwargs
    parts = event_name.split(".", 2)
    if len(parts) == 3:
        service = parts[1]
        operation = parts[2]
        service_key = _BOTO3_OP_MAP.get((service, operation))
        if service_key:
            _record_call(service_key)


def _patch_openai() -> None:
    try:
        import openai  # type: ignore
    except ImportError:
        return

    try:
        original_create = openai.chat.completions.create  # type: ignore

        @functools.wraps(original_create)
        def patched_create(*args, **kwargs):
            response = original_create(*args, **kwargs)
            try:
                usage = getattr(response, "usage", None)
                input_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
                output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
                model = kwargs.get("model", "")
                service_key = "openai_gpt4_turbo" if ("gpt-4-turbo" in model or "gpt-4o" in model) else "openai_gpt4"
                _record_call(service_key, count=1, input_tokens=input_tokens, output_tokens=output_tokens)
            except Exception:
                pass
            return response

        openai.chat.completions.create = patched_create  # type: ignore
    except AttributeError:
        pass


def _patch_anthropic() -> None:
    try:
        import anthropic as anthropic_module  # type: ignore
    except ImportError:
        return

    try:
        original_create = anthropic_module.resources.Messages.create  # type: ignore

        @functools.wraps(original_create)
        def patched_create(self, *args, **kwargs):
            response = original_create(self, *args, **kwargs)
            try:
                usage = getattr(response, "usage", None)
                input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
                output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
                model = kwargs.get("model", "") or getattr(response, "model", "")
                if "haiku" in model:
                    service_key = "bedrock_claude_3_haiku"
                elif "opus" in model:
                    service_key = "bedrock_claude_3_opus"
                else:
                    service_key = "anthropic_claude"
                _record_call(service_key, count=1, input_tokens=input_tokens, output_tokens=output_tokens)
            except Exception:
                pass
            return response

        anthropic_module.resources.Messages.create = patched_create  # type: ignore
    except AttributeError:
        pass
