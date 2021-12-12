SELECT A.src AS src, A.dst AS via1, C.src AS via2, C.dst AS dsr
FROM Graph AS A, Graph AS B, Graph AS C
WHERE A.dst = B.src AND B.dst = C.src