CREATE INDEX birth_year_idx ON users (birth_year);
CREATE INDEX country_idx ON users (country);

SELECT * FROM users
  WHERE birth_year = 1981 AND country = 'FR' ALLOW FILTERING
  WITH included_indexes = {birth_year_idx}
    AND excluded_indexes = {country_idx};
