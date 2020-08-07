require 'linkeddata'
require 'csv'

class InstructieHarvester
  ORG = RDF::Vocab::ORG
  FOAF = RDF::Vocab::FOAF
  SKOS = RDF::Vocab::SKOS
  DC = RDF::Vocab::DC
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  REGORG = RDF::Vocabulary.new("http://www.w3.org/ns/regorg#")
  MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
  NFO = RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#")
  NIE =  RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/01/19/nie#")
  DBPEDIA = RDF::Vocabulary.new("http://dbpedia.org/ontology/")
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  SV = RDF::Vocabulary.new('http://www.w3.org/2003/06/sw-vocab-status/ns#')

  MOB = RDF::Vocabulary.new("https://data.vlaanderen.be/ns/mobiliteit#")
  MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
  BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
  EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")
  LBLOD_MOW = RDF::Vocabulary.new("http://data.lblod.info/vocabularies/mobiliteit/")

  def initialize(input_verkeersborden, input_instructie)
    @repo = RDF::Graph.load(input_verkeersborden)
    @csv_path = input_instructie
    @output = "output/verkeersborden-combinaties.ttl"
  end


  def harvest
    index = 1
    begin
      RDF::Graph.new do |graph|
        ::CSV.foreach(@csv_path, {  headers: :first_row, encoding: 'utf-8', quote_char: "\""  }) do |row|
          statements = parse_row(index, row)
          if statements.length > 0
            graph.insert_statements(statements)
          end
          index += 1
        end
        query = %(
SELECT ?combinatie
WHERE {
      ?combinatie a <#{LBLOD_MOW.Verkeersbordcombinatie}>; <#{DC.hasPart}> ?part.
} GROUP BY ?combinatie HAVING (COUNT(?part) < 2)
)
        SPARQL.execute(query, graph).each_solution do |solution|
          puts "combinatie #{solution[:combinatie]} heeft maar 1 bord en wordt verwijderd"
          graph.delete([solution[:combinatie], nil, nil])
        end
        File.write(@output, graph.dump(:ttl), mode: 'w')
      end
    rescue Exception => e
      puts "error on line #{index}"
      raise e
    end
  end

  def find_verkeersbord(verkeersbord_code)
    if verkeersbord_code.nil?
      return nil
    end
    if index = verkeersbord_code.index("Type")
      extract = verkeersbord_code.slice(index+5,verkeersbord_code.length)
      puts "mapped #{verkeersbord_code} to G#{extract}"
      verkeersbord_code= "G#{extract}"
    end
    query = RDF::Query.new({
                             bord: {
                               RDF.type  => MOB['Verkeersbordconcept'],
                               SKOS.prefLabel => verkeersbord_code
                             }
                           })
    result = query.execute(@repo)
    if result.length === 1
      return result.first[:bord]
    else
      return nil
    end

  end
  def parse_row(index, row)
    uuid = row.first[1]
    if uuid
      row_iri = RDF::URI("http://data.lblod.info/verkeersbordconcept-combinaties/#{uuid}")
      verkeersbord_code = row["verkeersbord_code"]
      verkeersbord_iri = find_verkeersbord(verkeersbord_code)
      verkeersbord_instructie = row["instructie"]
      statements = []
      if verkeersbord_iri
        statements << RDF::Statement.new( row_iri, RDF.type, LBLOD_MOW["Verkeersbordcombinatie"])
        statements << RDF::Statement.new( row_iri, DC.hasPart, verkeersbord_iri )
        statements << RDF::Statement.new( row_iri, MU.uuid, RDF::Literal.new(uuid))
        statements << RDF::Statement.new(verkeersbord_iri, DC.description, RDF::Literal.new(verkeersbord_instructie))
      else
        puts "rij #{index} geen verkeersbord gevonden voor code #{verkeersbord_code.inspect}"
      end
      statements
    else
      puts "rij #{index} heeft geen uuid"
      []
    end
  end
end

harvester = InstructieHarvester.new('./output/verkeersborden.ttl', './input/verkeersmaatregel-templates.csv')
harvester.harvest
