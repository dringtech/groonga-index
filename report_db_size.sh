#!/usr/bin/sh
du -m ./$1/* | awk '
BEGIN { d = 0; i = 0; dc = 0; ic = 0;}
/0000101/ { d = d + $1; dc++; }
/0000103/ { i = i + $1; ic++; }
END {
  print  "*================================*";
  printf "| Data  %7.2f GiB in %3i files |\n", d / 1024, dc;
  printf "| Index %7.2f GiB in %3i files |\n", i / 1024, ic;
  print  "*================================*";
  printf "| Total %7.2f GiB in %3i files |\n", (d + i) / 1024, dc + ic;
  print  "*================================*";
}
'
