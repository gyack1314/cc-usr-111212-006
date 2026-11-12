"""核对不可变表文件和发布候选配置。"""
from .io import asset,unique

def summarize(v,root):
    unique(v['table_snapshots'],'id')
    for table in v['table_snapshots']:
        for ref in table['files']:asset(root,ref)
    if not isinstance(v['initial_release']['tables'],dict):raise ValueError('release表映射无效')
    return {'table_snapshots':len(v['table_snapshots']),'candidates':len(v['candidates']),'initial_tables':len(v['initial_release']['tables'])}
