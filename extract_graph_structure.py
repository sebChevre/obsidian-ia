import os
import logging
from neo4j import GraphDatabase
import subprocess

# —— Configuration ——
CONFIG = {
    "VAULT_PATH": r"C:\Users\sdi232\OBSIDIAN\SDI",
    "VAULT_ID": "SDI",
    "NEO4J_URI": "bolt://localhost:7687",
    "NEO4J_USER": "neo4j",
    "NEO4J_PASSWORD": "Seb%110978",
    "EXCLUDED_DIRS": {".git", ".obsidian", ".trash"},
    "EXCLUDED_FILES": {".gitignore"},
    "MAX_FILE_SIZE": 10_000_000  # 10 Mo
}

# —— Logging ——
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# —— Fonctions utilitaires ——
def normalize_path(path, base_path):
    """Normalise un chemin relatif à base_path, en remplaçant \ par /"""
    return os.path.relpath(path, base_path).replace("\\", "/")

def clear_database(session):
    """Efface tout le contenu de la base Neo4j (optionnel)"""
    session.run("MATCH (n) DETACH DELETE n")

def read_file_content(file_path, max_size=CONFIG["MAX_FILE_SIZE"]):
    """Lit le contenu d'un fichier .md, avec gestion des gros fichiers"""
    try:
        file_size = os.path.getsize(file_path)
        if file_size > max_size:
            logger.warning(f"Fichier trop gros ({file_size} bytes) : {file_path}")
            return "[Fichier trop gros pour être lu]"
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Erreur lors de la lecture de {file_path}: {e}")
        return ""

def get_git_dates_for_file(file_path, vault_path):
    """Récupère les dates Git d'un fichier"""
    try:
        relative_path = normalize_path(file_path, vault_path)
        cmd = ["git", "log", "--format=%aI", "--reverse", "--", relative_path]
        result = subprocess.run(cmd, cwd=vault_path, capture_output=True, text=True, encoding='utf-8')
        if result.returncode != 0:
            logger.warning(f"Git error for {relative_path}: {result.stderr}")
            return {"created_at": None, "modified_at": None}
        lines = result.stdout.strip().splitlines()
        if not lines:
            return {"created_at": None, "modified_at": None}
        created_at = lines[0]
        modified_at = lines[-1]
        return {"created_at": created_at, "modified_at": modified_at}
    except Exception as e:
        logger.error(f"Error getting Git dates for {file_path}: {e}")
        return {"created_at": None, "modified_at": None}

# —— Fonctions Neo4j ——
def create_or_update_note(session, path, name, level, vault_id, created_at, modified_at, contenu):
    try:
        session.run("""
            MERGE (note:Note {path: $path, name: $name, level: $level, vault_id: $vault_id})
            SET note.created_at = $created_at,
                note.modified_at = $modified_at,
                note.contenu = $contenu
            RETURN note
        """, path=path, name=name, level=level, vault_id=vault_id, created_at=created_at, modified_at=modified_at, contenu=contenu)
    except Exception as e:
        logger.error(f"Erreur lors de la création de la note {name}: {e}")

def create_parent_relation(session, parent_path, child_path, vault_id):
    try:
        session.run("""
            MATCH (parent:Dossier {path: $parent_path, vault_id: $vault_id})
            MATCH (child:Dossier {path: $child_path, vault_id: $vault_id})
            MERGE (parent)-[:PARENT_OF]->(child)
        """, parent_path=parent_path, child_path=child_path, vault_id=vault_id)
    except Exception as e:
        logger.error(f"Erreur lors de la création de la relation PARENT_OF entre {parent_path} et {child_path}: {e}")

# —— Fonction principale ——
def create_nodes_and_relations(session, vault_path):
    """
    Parcourt le vault Obsidian et crée les nœuds Dossier et Note dans Neo4j,
    avec leurs relations PARENT_OF et SIBLING_OF.
    
    Args:
        session (neo4j.Session): Session Neo4j active
        vault_path (str): Chemin absolu vers le vault Obsidian
    """
    vault_path = os.path.normpath(vault_path)
    root_name = os.path.basename(vault_path)

    # Créer le nœud racine
    session.run("""
        MERGE (root:Dossier {path: ".", name: $root_name, level: 0, vault_id: $vault_id})
        RETURN root
    """, root_name=root_name, vault_id=CONFIG["VAULT_ID"])

    for dirpath, dirnames, filenames in os.walk(vault_path):
        # Filtrer les dossiers exclus
        dirnames[:] = [d for d in dirnames if d not in CONFIG["EXCLUDED_DIRS"]]

        # Calculer le niveau
        relative_path = os.path.relpath(dirpath, vault_path)
        level = 0 if relative_path == "." else len(relative_path.split(os.sep))

        # Normaliser le chemin
        relative_dir_path = normalize_path(dirpath, vault_path)

        # Créer le nœud dossier
        session.run("""
            MERGE (dir:Dossier {path: $dir_path, name: $dir_name, level: $level, vault_id: $vault_id})
            RETURN dir
        """, dir_path=relative_dir_path, dir_name=os.path.basename(dirpath), level=level, vault_id=CONFIG["VAULT_ID"])

        # Lier au parent (sauf racine)
        if level > 0:
            parent_dir = os.path.dirname(relative_path)
            parent_path = "." if parent_dir == "" else parent_dir.replace("\\", "/")
            create_parent_relation(session, parent_path, relative_dir_path, CONFIG["VAULT_ID"])

        # Filtrer les fichiers exclus
        md_files = [
            f for f in filenames
            if f.endswith(".md") and f not in CONFIG["EXCLUDED_FILES"]
        ]
        file_nodes = []

        for filename in md_files:
            file_path = os.path.join(dirpath, filename)
            file_name = os.path.splitext(filename)[0]

            # Récupérer les dates Git
            git_info = get_git_dates_for_file(file_path, vault_path)
            created_at = git_info["created_at"]
            modified_at = git_info["modified_at"]

            logger.info(f"📄 Fichier à traiter : {file_path}")
            if created_at and modified_at:
                logger.info(f"✅ Dates trouvées : {created_at} / {modified_at}")
            else:
                logger.warning(f"❌ Aucune date trouvée pour {file_path}")

            # Chemin relatif
            relative_file_path = normalize_path(file_path, vault_path)

            # Lire le contenu
            contenu = read_file_content(file_path)

            # Créer la note
            create_or_update_note(session, relative_file_path, file_name, level, CONFIG["VAULT_ID"], created_at, modified_at, contenu)

            # Lier au dossier
            session.run("""
                MATCH (dir:Dossier {path: $dir_path, vault_id: $vault_id})
                MATCH (note:Note {path: $file_path, vault_id: $vault_id})
                MERGE (dir)-[:PARENT_OF]->(note)
            """, dir_path=relative_dir_path, file_path=relative_file_path, vault_id=CONFIG["VAULT_ID"])

            file_nodes.append(file_name)

        # Créer les relations frères
        if len(file_nodes) > 1:
            for i in range(len(file_nodes)):
                for j in range(i + 1, len(file_nodes)):
                    file1 = file_nodes[i]
                    file2 = file_nodes[j]
                    path1 = os.path.join(relative_dir_path, file1 + ".md").replace("\\", "/")
                    path2 = os.path.join(relative_dir_path, file2 + ".md").replace("\\", "/")
                    session.run("""
                        MATCH (n1:Note {name: $file1, path: $path1, vault_id: $vault_id})
                        MATCH (n2:Note {name: $file2, path: $path2, vault_id: $vault_id})
                        MERGE (n1)-[:SIBLING_OF]->(n2)
                        MERGE (n2)-[:SIBLING_OF]->(n1)
                    """, file1=file1, file2=file2, path1=path1, path2=path2, vault_id=CONFIG["VAULT_ID"])

# —— Exécution ——
def main():
    driver = GraphDatabase.driver(CONFIG["NEO4J_URI"], auth=(CONFIG["NEO4J_USER"], CONFIG["NEO4J_PASSWORD"]))
    with driver.session() as session:
        # Optionnel : effacer la base avant import
        clear_database(session)
        create_nodes_and_relations(session, CONFIG["VAULT_PATH"])
    driver.close()

if __name__ == "__main__":
    main()



