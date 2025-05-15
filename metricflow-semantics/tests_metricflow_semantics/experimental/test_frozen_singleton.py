# from __future__ import annotations
#
# import logging
# from dataclasses import dataclass
# from typing import ClassVar, Set
#
# logger = logging.getLogger(__name__)
#
#
# @dataclass(frozen=True)
# class SingletonDataclass:
#     field_0: int
#
#     _instances: ClassVar[Set["SingletonDataclass"]] = set()
#
#     def __new__(cls, *args, **kwargs):
#         instance = super().__new__(cls)
#         return instance
#
#
#
# def test_singleton() -> None:  # noqa: D
#     logger.info(SingletonDataclass(1))
