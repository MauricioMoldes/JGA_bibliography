CREATE SCHEMA bibliography;

-- ----------------------------
-- Table structure for article
-- ----------------------------
DROP TABLE IF EXISTS "bibliography"."article";
CREATE TABLE "bibliography"."article" (
  "article_id" int4 NOT NULL,
  "source" text COLLATE "pg_catalog"."default",
  "doi" text COLLATE "pg_catalog"."default",
  "title" text COLLATE "pg_catalog"."default",
  "author_string" text COLLATE "pg_catalog"."default",
  "journal_title" text COLLATE "pg_catalog"."default",
  "issue" text COLLATE "pg_catalog"."default",
  "journal_volume" text COLLATE "pg_catalog"."default",
  "publication_year" int4,
  "journal_issn" text COLLATE "pg_catalog"."default",
  "page_information" text COLLATE "pg_catalog"."default",
  "publication_type" text COLLATE "pg_catalog"."default",
  "cited_by" int4,
  "is_open_access" text COLLATE "pg_catalog"."default",
  "in_epmc" bool,
  "in_pmc" bool,
  "has_references" bool,
  "has_text_mined_terms" bool,
  "has_db_cross_references" bool,
  "has_labs_links" bool,
  "has_tma_accession_numbers" bool,
  "created" timestamp(6),
  "last_updated" timestamp(6),
  "first_publication_date" timestamp(6)
)
;

-- ----------------------------
-- Table structure for citation
-- ----------------------------
DROP TABLE IF EXISTS "bibliography"."citation";
CREATE TABLE "bibliography"."citation" (
  "src_article_id" int4 NOT NULL,
  "tgt_article_id" int4 NOT NULL
)
;

-- ----------------------------
-- Table structure for jga_study
-- ----------------------------
DROP TABLE IF EXISTS "bibliography"."jga_study";
CREATE TABLE "bibliography"."jga_study" (
  "jga_accession_id" varchar(50) COLLATE "pg_catalog"."default" NOT NULL
)
;

-- ----------------------------
-- Table structure for jga_study_article
-- ----------------------------
DROP TABLE IF EXISTS "bibliography"."jga_study_article";
CREATE TABLE "bibliography"."jga_study_article" (
  "jga_accession_id" varchar(50) COLLATE "pg_catalog"."default" NOT NULL,
  "article_id" int4 NOT NULL
)
;

-- ----------------------------
-- Primary Key structure for table article
-- ----------------------------
ALTER TABLE "bibliography"."article" ADD CONSTRAINT "article_pkey" PRIMARY KEY ("article_id");

-- ----------------------------
-- Primary Key structure for table citation
-- ----------------------------
ALTER TABLE "bibliography"."citation" ADD CONSTRAINT "citation_pkey" PRIMARY KEY ("src_article_id", "tgt_article_id");

-- ----------------------------
-- Primary Key structure for table jga_study
-- ----------------------------
ALTER TABLE "bibliography"."jga_study" ADD CONSTRAINT "jga_study_pkey" PRIMARY KEY ("jga_accession_id");

-- ----------------------------
-- Primary Key structure for table jga_study_article
-- ----------------------------
ALTER TABLE "bibliography"."jga_study_article" ADD CONSTRAINT "jga_study_article_pkey" PRIMARY KEY ("jga_accession_id", "article_id");

-- ----------------------------
-- Foreign Keys structure for table citation
-- ----------------------------
ALTER TABLE "bibliography"."citation" ADD CONSTRAINT "citation_src_article_id_fkey" FOREIGN KEY ("src_article_id") REFERENCES "bibliography"."article" ("article_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "bibliography"."citation" ADD CONSTRAINT "citation_tgt_article_id_fkey" FOREIGN KEY ("tgt_article_id") REFERENCES "bibliography"."article" ("article_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table jga_study_article
-- ----------------------------
ALTER TABLE "bibliography"."jga_study_article" ADD CONSTRAINT "jga_study_article_article_id_fkey" FOREIGN KEY ("article_id") REFERENCES "bibliography"."article" ("article_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "bibliography"."jga_study_article" ADD CONSTRAINT "jga_study_article_jga_accession_id_fkey" FOREIGN KEY ("jga_accession_id") REFERENCES "bibliography"."jga_study" ("jga_accession_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
