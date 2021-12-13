CREATE VIEW GraphWindowed AS
SELECT src, dst, window_start, window_end
FROM TABLE(TUMBLE(TABLE Graph, DESCRIPTOR(ts), INTERVAL '4' MINUTES))
GROUP BY src, dst, window_start, window_end