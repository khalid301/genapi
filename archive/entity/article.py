from pydantic import BaseModel
from typing import List, Optional


class Artikel(BaseModel):
    id: int = 0
    judul: str = ""
    konten: Optional[str] = None
    gambar_artikel: Optional[List["GambarArtikel"]] = []


class GambarArtikel(BaseModel):
    id: int = 0
    artikel_id: int = 0
    caption: Optional[str] = None
    file: str = ""


class GetArticlesRequest(BaseModel):
    page: int = 1
    page_size: int = 10
    keyword: Optional[str] = None
    with_gambar: Optional[bool] = False


class GetArticleByIDRequest(BaseModel):
    id: int = 0
    with_gambar: Optional[bool] = False


class GetGambarArticlesByArticleIDs(BaseModel):
    artikel_ids: list[int]
    with_gambar: bool = True
