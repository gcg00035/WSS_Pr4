"""
Módulo avanzado para poblar automáticamente una ontología OWL usando RDFlib y Pandas.
Lee datos desde dos archivos CSV (empresas y personas) y genera individuos,
asignando múltiples clases y Object Properties complejas.
"""

import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, OWL, XSD

def poblar_ontologia_completa(ruta_base: str, csv_empresas: str, csv_personas: str, ruta_salida: str) -> None:
    """
    Lee archivos CSV con datos corporativos e inyecta las instancias en la ontología OWL.

    Args:
        ruta_base (str): Ruta al archivo de la ontología original (.owl).
        csv_empresas (str): Ruta al CSV con la estructura corporativa.
        csv_personas (str): Ruta al CSV con los datos de personal.
        ruta_salida (str): Ruta donde se guardará la nueva ontología poblada.
    """
    # 1. Inicializar el grafo y parsear la ontología
    g = Graph()
    g.parse(ruta_base, format="xml")

    # 2. Definir el Namespace exacto
    URI_BASE = "http://www.semanticweb.org/admin/ontologies/2026/2/untitled-ontology-3#"
    EMP = Namespace(URI_BASE)

    # ==========================================
    # FASE 1: PROCESAR EMPRESAS Y SU ESTRUCTURA
    # ==========================================
    df_empresas = pd.read_csv(csv_empresas)

    for _, row in df_empresas.iterrows():
        # Crear URI de la Empresa
        emp_uri = EMP[str(row['Nombre_Empresa']).replace(" ", "_")]
        g.add((emp_uri, RDF.type, OWL.NamedIndividual))

        # Sector
        if pd.notna(row['Sector']):
            sector_uri = EMP[str(row['Sector']).replace(" ", "_")]
            g.add((sector_uri, RDF.type, EMP.Sector))
            g.add((sector_uri, RDF.type, OWL.NamedIndividual))
            g.add((emp_uri, EMP.Se_dedica_a, sector_uri))

        # Sede y País
        if pd.notna(row['Sede']) and pd.notna(row['Pais']):
            sede_uri = EMP[str(row['Sede']).replace(" ", "_")]
            pais_uri = EMP[str(row['Pais']).replace(" ", "_")]

            # Instanciar clases
            g.add((sede_uri, RDF.type, EMP.Sede))
            g.add((sede_uri, RDF.type, OWL.NamedIndividual))
            g.add((pais_uri, RDF.type, EMP.País))
            g.add((pais_uri, RDF.type, OWL.NamedIndividual))

            # Relaciones: Empresa -> Sede -> País
            g.add((emp_uri, EMP.tiene, sede_uri))
            g.add((sede_uri, EMP.ubicada_en, pais_uri))

        # Filiales y Matriz
        if pd.notna(row['Matriz']):
            matriz_uri = EMP[str(row['Matriz']).replace(" ", "_")]
            g.add((emp_uri, RDF.type, EMP.Filial)) # Marcamos como filial
            g.add((emp_uri, RDF.type, EMP.Empresa))
            g.add((matriz_uri, EMP.tiene_filial, emp_uri)) # La matriz tiene a esta filial
        else:
            g.add((emp_uri, RDF.type, EMP.Empresa)) # Empresa normal

    # ==========================================
    # FASE 2: PROCESAR PERSONAS Y EMPLEADOS
    # ==========================================
    df_personas = pd.read_csv(csv_personas)

    for _, row in df_personas.iterrows():
        persona_uri = EMP[str(row['Nombre']).replace(" ", "_")]
        g.add((persona_uri, RDF.type, OWL.NamedIndividual))

        # Asignar Clase Principal (Trabajador, Directivo o Accionista puro)
        rol = str(row['Rol']).strip()
        if rol == 'Directivo':
            g.add((persona_uri, RDF.type, EMP.Directivo))
        elif rol == 'Trabajador':
            g.add((persona_uri, RDF.type, EMP.Trabajador))
        elif rol == 'Accionista':
            g.add((persona_uri, RDF.type, EMP.Accionista))

        # Propiedades de Datos
        g.add((persona_uri, EMP.nombre, Literal(row['Nombre'], datatype=XSD.string)))
        if pd.notna(row['Salario']):
            g.add((persona_uri, EMP.salario, Literal(float(row['Salario']), datatype=XSD.float)))

        # Empresa Asociada (Dónde trabaja, dirige o de quién es dueño)
        if pd.notna(row['Empresa_Asociada']):
            empresa_uri = EMP[str(row['Empresa_Asociada']).replace(" ", "_")]

            if rol == 'Directivo':
                g.add((persona_uri, EMP.dirige, empresa_uri))
            elif rol == 'Trabajador':
                g.add((persona_uri, EMP.trabaja_en, empresa_uri))

            # ¿Es también accionista?
            if str(row['Es_Accionista']).strip().upper() == 'SI':
                g.add((persona_uri, RDF.type, EMP.Accionista))
                g.add((persona_uri, EMP.es_propietario_de, empresa_uri))

            # Departamento
            if pd.notna(row['Departamento']):
                depto_uri = EMP[str(row['Departamento']).replace(" ", "_")]
                g.add((depto_uri, RDF.type, EMP.Departamento))
                g.add((depto_uri, RDF.type, OWL.NamedIndividual))

                # El trabajador está asignado al departamento
                g.add((persona_uri, EMP.asignado_a, depto_uri))
                # La empresa.owl está compuesta por ese departamento
                g.add((empresa_uri, EMP.compuesta_por, depto_uri))

    # 3. Guardar la nueva ontología
    g.serialize(destination=ruta_salida, format="xml")
    print(f"Ontología enriquecida guardada exitosamente en: {ruta_salida}")

if __name__ == "__main__":
    # Nombres de los archivos
    poblar_ontologia_completa("empresasRDF.rdf", "empresas.csv", "personas.csv", "empresa_poblada.owl")