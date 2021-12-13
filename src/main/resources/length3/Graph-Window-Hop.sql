CREATE VIEW GraphWindowed AS
SELECT src, dst, window_start, window_end
FROM TABLE(HOP(TABLE Graph, DESCRIPTOR(ts), INTERVAL '2' MINUTES, INTERVAL '4' MINUTES))
GROUP BY src, dst, window_start, window_end