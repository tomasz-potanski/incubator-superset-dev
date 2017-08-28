#!/usr/bin/python
# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Table, MetaData, sql
from sqlalchemy.sql import select
from sqlalchemy.schema import Column
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.dialects.postgresql import DATE, VARCHAR, DOUBLE_PRECISION, BIGINT, INTEGER, NUMERIC, ARRAY, array
from sqlalchemy.sql.expression import text

metadata = MetaData()
km_sample_src = Table('km_sample_src', metadata,
Column('pid', INTEGER),
Column('points', ARRAY(DOUBLE_PRECISION)),
# Column('points', DOUBLE_PRECISION),
)

# km_sample_result = Table('km_sample_result', metadata,
# Column('centroids', ARRAY(DOUBLE_PRECISION)),
# Column('cluster_variance', ARRAY(DOUBLE_PRECISION)),
# Column('objective_fn', DOUBLE_PRECISION),
# Column('frac_reassigned', DOUBLE_PRECISION),
# Column('num_iterations', INTEGER),
# )

eng = create_engine("postgresql://postgres:postgres@localhost:5433/superset_madlib", echo=True)
conn = eng.connect()
session = sessionmaker(bind=eng)()
Base = declarative_base()

command = "DROP TABLE IF EXISTS {};".format('km_sample_src')
session.execute( command )
command = "DROP TABLE IF EXISTS {};".format('km_sample_result')
session.execute( command )
command = "DROP TABLE IF EXISTS {};".format('km_sample_cluster_result')
session.execute( command )
session.commit()
# km_sample_src.drop(eng)
# km_sample_src.create(eng)
# km_sample_result.drop(eng)
# km_sample_result.create(eng)
metadata.create_all(eng)

class Km_sample(Base):
    __tablename__ = 'km_sample'
    pid = Column('pid', INTEGER, primary_key=True)
    points1 = Column('points1', DOUBLE_PRECISION)
    points2 = Column('points2', DOUBLE_PRECISION)
    points3 = Column('points3', DOUBLE_PRECISION)
    points4 = Column('points4', DOUBLE_PRECISION)
    points5 = Column('points5', DOUBLE_PRECISION)
    points6 = Column('points6', DOUBLE_PRECISION)
    points7 = Column('points7', DOUBLE_PRECISION)
    points8 = Column('points8', DOUBLE_PRECISION)
    points9 = Column('points9', DOUBLE_PRECISION)
    points10 = Column('points10', DOUBLE_PRECISION)
    points11 = Column('points11', DOUBLE_PRECISION)
    points12 = Column('points12', DOUBLE_PRECISION)
    points13 = Column('points13', DOUBLE_PRECISION)


col_list = [
		Km_sample.points1,
		Km_sample.points2,
		Km_sample.points3,
		Km_sample.points4,
		Km_sample.points5,
		Km_sample.points6,
		Km_sample.points7,
		Km_sample.points8,
		Km_sample.points9,
		Km_sample.points10,
		Km_sample.points11,
		Km_sample.points12,
		Km_sample.points13,
	]

points_array = array([Km_sample.points1])
for n, point in enumerate(col_list):
	points_array += array([point])

# for row in session.query(Km_sample) \
#                   .order_by(Km_sample.pid):
#     print(row.pid, row.points1)

# s = select( [ Km_sample.pid, array([Km_sample.points1, Km_sample.points2]) ] )
# result = conn.execute(s)
# print(result.fetchall())
s = km_sample_src.insert().from_select(
    ['pid', 'points'],
    select([
        Km_sample.pid,
	points_array
#         array([
#             Km_sample.points1,
#             Km_sample.points2,
#             Km_sample.points3,
#             Km_sample.points4,
#             Km_sample.points5,
#             Km_sample.points6,
#             Km_sample.points7,
#         ]) +
#         array([
#             Km_sample.points8,
#             Km_sample.points9,
#             Km_sample.points10,
#             Km_sample.points11,
#             Km_sample.points12,
#             Km_sample.points13,
#         ])
    ])
)
conn.execute(s)

# http://docs.sqlalchemy.org/en/rel_1_1/orm/query.html?highlight=select_from#sqlalchemy.orm.query.Query.select_entity_from

# stmt = text("select * from madlib.kmeanspp('km_sample_src', 'points', 3, :dist_fn, :agg_fn, 20, 0.001)", {'dist_fn': 'madlib.squared_dist_norm2', 'agg_fn': 'madlib.avg'})
# stmt = stmt(
#         km_sample_result.centroids,
#         km_sample_result.cluster_variance,
#         km_sample_result.objective_fn,
#         km_sample_result.frac_reassigned,
#         km_sample_result.num_iterations)
session.execute("CREATE TABLE km_sample_result AS select * from madlib.kmeanspp('km_sample_src', 'points', :num_centroid, :dist_fn, :agg_fn, :max_iter, :min_frac)",
		{'num_centroid': 3, 'dist_fn': 'madlib.squared_dist_norm2', 'agg_fn': 'madlib.avg', 'max_iter': 20, 'min_frac': 0.001 })
session.commit()
# s = km_sample_result.insert().from_select(['centroids', 'cluster_variance', 'objective_fn', 'frac_reassigned', 'num_iterations'], )
# conn.execute(s)

# session.query(km_sample_result).from_statement(stmt).all()

session.execute("CREATE TABLE km_sample_cluster_result AS SELECT data.*,  (madlib.closest_column(centroids, points)).column_id as cluster_id FROM km_sample_src as data, km_sample_result ORDER BY data.pid")
session.commit()
