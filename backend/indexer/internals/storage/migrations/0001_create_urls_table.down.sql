-- Drop tables in dependency order (child tables first)
DROP TABLE IF EXISTS word_data;
DROP TABLE IF EXISTS inverted_index;
DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS robot_rules;
DROP TABLE IF EXISTS urls;
