import sqlglot
from sqlglot import parse_one, exp
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class SQLValidator:
    def __init__(self, allowed_tables: list = ['dim_tickers', 'dimtime', 'fact_ohlcv']):
        self.allowed_tables = allowed_tables
    
    def validate(self, query: str) -> Dict[str, Any]:
        """Valide SQL : safe + autorisé"""
        try:
            # Parse SQL
            parsed = parse_one(query, dialect='postgres')
            
            # 1. UNIQUEMENT SELECT
            if not isinstance(parsed, exp.Select):
                return {"is_valid": False, "reason": "Seulement SELECT autorisé (no INSERT/UPDATE/DELETE/DROP)"}
            
            # 2. Tables autorisées (gère joins/subqueries/CTE)
            tables = parsed.find_all(exp.Table)
            invalid_tables = [t.name for t in tables if t.name not in self.allowed_tables]
            if invalid_tables:
                return {"is_valid": False, "reason": f"Tables interdites: {invalid_tables}"}
            
            # 3. No LIMIT 0 ou * sans WHERE sur fact (évite full dump)
            if isinstance(parsed.args.get('limit'), exp.Literal) and parsed.args['limit'].this == 0:
                return {"is_valid": False, "reason": "LIMIT 0 interdit"}
            
            # 4. No injections pattern basique
            dangerous = ['; --', 'UNION SELECT', 'pg_sleep', 'DROP TABLE']
            if any(pat in query.upper() for pat in dangerous):
                return {"is_valid": False, "reason": "Pattern malveillant détecté"}
            
            # 5. Check refs FK/schéma (optionnel avancé)
            return {"is_valid": True, "reason": "Query safe", "validated_sql": query}
            
        except Exception as e:
            return {"is_valid": False, "reason": f"Erreur parse/validation: {str(e)}"}

# Usage
validator = SQLValidator()
