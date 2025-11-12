import pandas as pd
from pathlib import Path

p_raiz = Path("dataset")
p_bronze = p_raiz / "bronze"
p_silver = p_raiz / "silver"


def ver_dados():
    arqs = list(p_bronze.rglob("*.parquet"))
    
    if not arqs:
        print("Sem arquivos!")
        return None
    
    print(f"Arquivos: {len(arqs)}")
    
    df = pd.read_parquet(arqs[0])
    
    print("\nColunas:")
    print(df.dtypes)
    
    print("\nPrimeiras linhas:")
    print(df.head(3))
    
    print("\nNulos:")
    for col in df.columns:
        n = df[col].isnull().sum()
        p = (n / len(df)) * 100
        print(f"{col}: {n} ({p:.1f}%)")
    
    return df


def limpar():
    arqs = list(p_bronze.rglob("*.parquet"))
    
    if not arqs:
        return
    
    tot = 0
    rem = 0
    
    for arq in arqs:
        print(f"\n{arq.name}")
        
        df = pd.read_parquet(arq)
        orig = len(df)
        
        df = df.drop_duplicates()
        df = df.dropna(subset=['ano', 'mes', 'valor', 'data_pagamento'])
        
        df['ano'] = pd.to_numeric(df['ano'], errors='coerce').astype('Int64')
        df['mes'] = pd.to_numeric(df['mes'], errors='coerce').astype('Int64')
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        df['data_pagamento'] = pd.to_datetime(df['data_pagamento'], errors='coerce')
        
        df = df.dropna(subset=['ano', 'mes', 'valor', 'data_pagamento'])
        
        cols = ['nome_orgao_superior', 'nome_orgao_subordinado', 
                'nome_unidade_gestora', 'favorecido', 'categoria']
        
        for c in cols:
            if c in df.columns:
                df[c] = df[c].astype(str).str.strip().str.upper()
                df[c] = df[c].replace('NAN', pd.NA)
        
        df = df[df['valor'] > 0]
        df = df[(df['mes'] >= 1) & (df['mes'] <= 12)]
        
        df['ano_mes'] = df['ano'].astype(str) + '-' + df['mes'].astype(str).str.zfill(2)
        df['trimestre'] = ((df['mes'] - 1) // 3 + 1).astype('Int64')
        
        if 'cpf_cnpj_favorecido' in df.columns:
            df['cpf_cnpj_favorecido'] = df['cpf_cnpj_favorecido'].astype(str).str.replace(r'\D', '', regex=True)
            df['tipo_pessoa'] = df['cpf_cnpj_favorecido'].apply(
                lambda x: 'PJ' if len(x) == 14 else 'PF' if len(x) == 11 else 'INV'
            )
        
        final = len(df)
        dif = orig - final
        
        print(f"Original: {orig} | Limpo: {final} | Removido: {dif}")
        
        p_dest = p_silver / arq.relative_to(p_bronze)
        p_dest.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p_dest, index=False, engine="pyarrow")
        
        tot += final
        rem += dif
    
    print(f"\nTotal: {tot:,} | Removidos: {rem:,}")


def executar():
    ver_dados()
    limpar()


if __name__ == "__main__":
    executar()