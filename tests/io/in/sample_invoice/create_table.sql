CREATE TABLE invoice(
  id serial primary key,
  name VARCHAR(355) not null,
  amount double precision not null,
  "Remark" VARCHAR (355)
);