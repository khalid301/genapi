from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base
from archive.entity.article import Artikel as ArtikelEntity, GambarArtikel as GambarArtikelEntity

Base = declarative_base()


class Artikel(Base):
    __tablename__ = 'artikel'

    id = Column(Integer(), primary_key=True)
    judul = Column(String(255), nullable=True)
    konten = Column(Text(), nullable=True)

    def to_entity(self) -> ArtikelEntity:
        return ArtikelEntity(
            id=self.id,
            judul=self.judul,
            konten=self.konten
        )

    def from_entity(self, entity_rec: ArtikelEntity):
        self.id = entity_rec.id
        self.judul = entity_rec.judul
        self.konten = entity_rec.konten


class GambarArtikel(Base):
    __tablename__ = 'gambar_artikel'

    id = Column(Integer(), primary_key=True)
    artikel_id = Column(Integer(), nullable=False)
    caption = Column(String(255), nullable=True)
    file = Column(String(255), nullable=False)

    def to_entity(self) -> GambarArtikelEntity:
        return GambarArtikelEntity(
            id=self.id,
            artikel_id=self.artikel_id,
            caption=self.caption,
            file=self.file
        )

    def from_entity(self, entity_rec: GambarArtikelEntity):
        self.id = entity_rec.id
        self.artikel_id = entity_rec.artikel_id
        self.caption = entity_rec.caption
        self.file = entity_rec.file
