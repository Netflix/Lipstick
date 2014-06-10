fruits = load './1.dat' using PigStorage(',') as (id:int, fruit:chararray);
names = load './2.dat' using PigStorage(',') as (id:int, name:chararray);
names = filter names by id == 1 and not id == 2 or id * .5 == 1 and name == 'jeff';
fruit_names_join = join fruits by id, names by id;
limited = limit fruit_names_join 100;
fruit_names = foreach limited generate fruit, UPPER(name);
no_fruit_names = FILTER fruit_names by 1 == 2;
dump no_fruit_names;
--store fruit_names into 'blah2013';

