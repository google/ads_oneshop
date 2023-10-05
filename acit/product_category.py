# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Optional, Dict

from importlib import resources

_ID_CACHE: Dict[int, str] = {}
_PATH_CACHE: Dict[str, int] = {}
_FILE_NAME = 'taxonomy-with-ids.en-US.txt'


def _load():
  if not _ID_CACHE:
    with resources.open_text(__name__.rsplit('.', 1)[0], _FILE_NAME) as f:
      for line in f:
        if line.startswith('#'):
          continue
        id_, path = line.strip().split(' - ')
        id_ = int(id_)
        _ID_CACHE[id_] = path
        _PATH_CACHE[path] = id_


def path_by_id(id_: int) -> Optional[str]:
  _load()
  return _ID_CACHE.get(id_)


def id_by_path(path: str) -> Optional[int]:
  _load()
  return _PATH_CACHE.get(path)
