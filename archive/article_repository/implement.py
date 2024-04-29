from abc import ABC
from archive.entity.article import Artikel as ArtikelEntity, GambarArtikel as GambarArtikelEntity
from .dto import Artikel, GambarArtikel
from pkg.conn.database import engine
from sqlalchemy.orm import sessionmaker
from archive.entity.article import GetArticlesRequest, GetArticleByIDRequest, GetGambarArticlesByArticleIDs


class ArticleRepository(ABC):
    def __init__(self) -> None:
        super().__init__()

        _session = sessionmaker(bind=engine)
        self.session = _session()

    async def get_articles(self, req: GetArticlesRequest) -> list[ArtikelEntity]:
        articles = self.session.query(Artikel).all()

        return [article.to_entity() for article in articles]

    async def get_article_by_id(self, req: GetArticleByIDRequest) -> ArtikelEntity:
        artikel = self.session.query(Artikel).filter(Artikel.id == req.id).first()

        return artikel.to_entity()

    async def get_gambar_articles_by_artikel_ids(self, req: GetGambarArticlesByArticleIDs) -> list[GambarArtikelEntity]:
        gambar_artikel = self.session.query(GambarArtikel).filter(GambarArtikel.artikel_id.in_(req.artikel_ids)).all()

        final_return = []
        for gambar in gambar_artikel:
            final_return.append(gambar.to_entity())

        if not req.with_gambar:
            for ret in final_return:
                del ret.file

        return final_return
