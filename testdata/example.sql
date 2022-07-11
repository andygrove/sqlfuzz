SELECT _c20, _c21, _c22, _c23
FROM (SELECT test0.c0 AS _c20, test0.c1 AS _c21, test0.c2 AS _c22, test0.c3 AS _c23
    FROM (test0))
WHERE EXISTS (SELECT _c24
    FROM (SELECT _c24, _c25, _c26, _c27
    FROM (SELECT test0.c0 AS _c24, test0.c1 AS _c25, test0.c2 AS _c26, test0.c3 AS _c27
    FROM (test0)))
    WHERE _c24 = _c20);