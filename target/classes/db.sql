CREATE TABLE tv (
                         id serial primary key not null ,
                         manufacturer varchar(30),
                         model varchar(30),
                         diagonal varchar(30),
                         resolution varchar(10),
                         matrix varchar(10),
						 wifi boolean,
						 smarttv varchar(10)
);

CREATE TABLE monitor (
                         id serial primary key not null ,
                         manufacturer varchar(30),
                         model varchar(30),
                         diagonal varchar(30),
                         resolution varchar(10),
                         matrix varchar(10),
						 freshrate int,
						 synctype varchar(20)
);