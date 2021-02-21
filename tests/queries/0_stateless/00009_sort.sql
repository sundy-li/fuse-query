select 'normal';
select number from system.numbers_mt(3) order by number;
select number from system.numbers_mt(3) order by number desc;
select 'limit';
select number from system.numbers_mt(10000) order by number limit 3;
select number from system.numbers_mt(10000) order by number desc limit 3;
select 'other';
select number - 7 as d from system.numbers_mt(10000) order by d desc limit 3;
