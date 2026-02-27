SELECT DISTINCT battle_time, opponent_tag 
    FROM battles
    ORDER BY battle_time DESC
    LIMIT 1000;

ALTER TABLE battles
ALTER COLUMN battle_time TYPE timestamp 
