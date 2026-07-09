"""Throwaway spike probe: does grpc survive Nuitka standalone freezing on Windows?

Not production code. Exists only to get a CI signal on real Windows runners for
the risk raised in PR #2072 (Paul's suggestion to use gRPC for the mf-ipc
protocol). Already validated on macOS arm64 locally, including combined with
real MetricFlow deps and real protobuf message serialization — this is the
one remaining untested platform. Delete this directory once that signal lands.
"""
from __future__ import annotations

from concurrent import futures

import grpc  # type: ignore[import-not-found]


def _echo(request: bytes, context: grpc.ServicerContext) -> bytes:
    return request


class _EchoHandler(grpc.GenericRpcHandler):
    def service(self, handler_call_details: grpc.HandlerCallDetails) -> grpc.RpcMethodHandler:
        return grpc.unary_unary_rpc_method_handler(
            _echo,
            request_deserializer=lambda x: x,
            response_serializer=lambda x: x,
        )


def main() -> None:
    """Run a real grpc server/client RPC round trip and assert the response matches."""
    print(f"grpc version: {grpc.__version__}")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers((_EchoHandler(),))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    print(f"server listening on 127.0.0.1:{port}")

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = channel.unary_unary("/probe/Echo")
    response = stub(b"hello from windows spike")
    assert response == b"hello from windows spike", f"unexpected response: {response!r}"
    print(f"RPC round-trip OK: {response!r}")

    channel.close()
    server.stop(None).wait(timeout=5)
    print("OK")


if __name__ == "__main__":
    main()
