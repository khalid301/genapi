from archive.entity.article import Artikel, GetArticlesRequest, GetArticleByIDRequest, GetGambarArticlesByArticleIDs
from archive.article_repository.implement import ArticleRepository


class ArticleUseCase:
    def __init__(self) -> None:
        super().__init__()
        self.ArticleRepo = ArticleRepository()

    async def get_articles(self, req: GetArticlesRequest) -> list[Artikel]:
        articles = await self.ArticleRepo.get_articles()

        # get_gambar_articles_by_artikel_ids
        artikel_ids = [article.id for article in articles]
        gambar_articles_req = GetGambarArticlesByArticleIDs(artikel_ids=artikel_ids, with_gambar=req.with_gambar)
        gambar_articles = await self.ArticleRepo.get_gambar_articles_by_artikel_ids(gambar_articles_req)

        # create dict gambar_articles with artikel_id as key
        gambar_articles_dict = {}
        for gambar in gambar_articles:
            if gambar.artikel_id not in gambar_articles_dict:
                gambar_articles_dict[gambar.artikel_id] = []
            gambar_articles_dict[gambar.artikel_id].append(gambar)

        # assign gambar_articles to articles
        for article in articles:
            article.gambar_artikel = gambar_articles_dict.get(article.id, [])

        return articles

    async def get_article_by_id(self, req: GetArticleByIDRequest) -> Artikel:
        article = await self.ArticleRepo.get_article_by_id(req)

        # get_gambar_articles_by_artikel_ids
        artikel_ids = [article.id]
        gambar_articles_req = GetGambarArticlesByArticleIDs(artikel_ids=artikel_ids, with_gambar=req.with_gambar)
        gambar_articles = await self.ArticleRepo.get_gambar_articles_by_artikel_ids(gambar_articles_req)

        # create dict gambar_articles with artikel_id as key
        gambar_articles_dict = {}
        for gambar in gambar_articles:
            if gambar.artikel_id not in gambar_articles_dict:
                gambar_articles_dict[gambar.artikel_id] = []
            gambar_articles_dict[gambar.artikel_id].append(gambar)

        # assign gambar_articles to article
        article.gambar_artikel = gambar_articles_dict.get(article.id, [])

        return article
