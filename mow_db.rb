require 'linkeddata'
require 'csv'
require 'digest'

# Harvest Road_Sign_Concepts defined as Verkeersbordconcept
# in application profile https://data.vlaanderen.be/doc/applicatieprofiel/verkeersborden/#Verkeersbordconcept
# from mow database
class Road_Sign_ConceptHarvester
  DC = RDF::Vocab::DC
  FOAF = RDF::Vocab::FOAF
  ORG = RDF::Vocab::ORG
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  SKOS = RDF::Vocab::SKOS
  VS = RDF::Vocab::VS
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  DBPEDIA = RDF::Vocabulary.new('http://dbpedia.org/ontology/')
  MU = RDF::Vocabulary.new('http://mu.semte.ch/vocabularies/core/')
  NFO = RDF::Vocabulary.new('http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#')
  NIE = RDF::Vocabulary.new('http://www.semanticdesktop.org/ontologies/2007/01/19/nie#')
  REGORG = RDF::Vocabulary.new('https://www.w3.org/ns/regorg#')

  BESLUIT = RDF::Vocabulary.new('http://data.vlaanderen.be/ns/besluit#')
  EXT = RDF::Vocabulary.new('http://mu.semte.ch/vocabularies/ext/')
  LBLOD_MOW = RDF::Vocabulary.new('http://data.lblod.info/vocabularies/mobiliteit/')
  MANDAAT = RDF::Vocabulary.new('http://data.vlaanderen.be/ns/mandaat#')
  MOB = RDF::Vocabulary.new('https://data.vlaanderen.be/ns/mobiliteit#')

  MOW_BASE_URI = 'http://mow.lblod.info/%{resource}/%{id}'
  DATA_GIFT = 'http://mobiliteit.vo.data.gift/images/%{id}'
  CONCEPT_BASE_URI = 'http://data.vlaanderen.be/id/concept/%{scheme_name}/%{id}'
  SCHEME_BASE_URI = 'http://data.vlaanderen.be/id/conceptscheme/%{scheme_name}'

  @@road_sign_categorie_name_map = {
    'A' => 'Gevaarsbord',
    'B' => 'Voorrangsbord',
    'C' => 'Verbodsbord',
    'D' => 'Gebodsbord',
    'E' => 'StilstaanParkeerBord',
    'F' => 'Aanwijsbord',
    'G' => 'Onderbord',
    'M' => 'OnderbordFietsen',
    'T' => 'Afbakeningsbord', # Type
    'I' => 'InternAWVbord',
    'X' => 'Voertuig',
    'Z' => 'Zonebord'
  }

  @@road_sign_categorie_uri_map = {}

  @@road_sign_status_map = {}

  def initialize(input_file, output_file)
    @input_file = input_file
    @output_file = output_file
    @graph = RDF::Graph.new
  end

  def harvest
    insert_road_sign_categories
    @@road_sign_status_map = insert_verkeersbordconcept_status_codes
    index = 1
    RDF::Graph.new do |_graph|
      ::CSV.foreach(@input_file, { headers: :first_row, encoding: 'iso-8859-1', quote_char: '"' }) do |row|
        parse_row(index, row)
        index += 1
      end
    end
    File.write(@output_file, @graph.dump(:ttl), mode: 'w')
  end

  # TODO:
  # - images
  def parse_row(index, row)
    road_sign_concept = road_sign_concept(index, row['Bordcode'])

    subject = road_sign_concept['uri']
    meaning = attribute(index, row, 'betekenis')
    application = attribute(index, row, 'toepassing')
    note = attribute(index, row, 'opmerking')
    category_uri = @@road_sign_categorie_uri_map[road_sign_concept['code'][0]]
    conceptscheme_uri = RDF::URI(format(SCHEME_BASE_URI, scheme_name: 'Verkeersbordconcept'))

    @graph << RDF.Statement(subject, RDF.type, MOB.Verkeersbordconcept)
    @graph << RDF.Statement(subject, MU.uuid, road_sign_concept['uuid'])
    @graph << RDF.Statement(subject, SKOS.prefLabel, road_sign_concept['code'])
    !meaning.nil? && @graph << RDF.Statement(subject, SKOS.scopeNote, meaning)
    !category_uri.nil? && @graph << RDF.Statement(subject, ORG.classification, category_uri)
    @graph << RDF.Statement(subject, SKOS.topConceptOf, conceptscheme_uri)
    @graph << RDF.Statement(subject, SKOS.inScheme, conceptscheme_uri)

    #app.rb logic:
    status_uri = insert_verkeersbordconcept_status(@@road_sign_status_map['stabiel'], road_sign_concept['code'])
    @graph << RDF.Statement(subject, VS.term_status, status_uri)

    # Not yet formalized in application profile (2021-02)
    !application.nil? && @graph << RDF.Statement(road_sign_concept['uri'], SKOS.definition, application)
    !note.nil? && @graph << RDF.Statement(road_sign_concept['uri'], SKOS.note, note)

    insert_related_and_sub_signs(index, row, road_sign_concept)
  end

  def road_sign_concept(index, code)
    canonical_code = canonical_code(index, code)
    salt = '2864a54c-ad29-49ae-8e38-0f2f790f9f46'
    uuid = hash("#{salt}:#{canonical_code}")
    uri = RDF::URI(format(CONCEPT_BASE_URI, scheme_name: 'Verkeersbordconcept', id: uuid))
    {
      'uri' => uri,
      'code' => canonical_code,
      'uuid' => uuid,
      'is_sub_sign' => (canonical_code.start_with? 'G') || (canonical_code.start_with? 'M')
    }
  end

  def canonical_code(index, code)
    # e.g. canonical_code: C01 --> C1
    m = code.match(/(?<start>[A-Z]+)0*(?<end>[1-9].*)/)
    if m
      canonical_code = m.named_captures['start'] + m.named_captures['end']
    else
      canonical_code = code
      puts "Failed to create canonical code for #{code} for index #{index}"
    end
    canonical_code
  end

  def attribute(index, row, attribute)
    val = row[attribute]
    val.nil? && (puts "No #{attribute} found for #{row['Bordcode']} for index #{index}")
    val
  end

  def scrape_road_signs(index, row)
    codes = Set[]
    %w[betekenis toepassing opmerking].each do |column|
      codes.merge(row[column].to_s.scan(/([A-Z]+\d+\w*)/))
    end
    codes.map { |code| road_sign_concept(index, code[0]) }
  end

  def insert_related_and_sub_signs(index, row, road_sign_concept)
    scrape_road_signs(index, row).each do |scraped_sign|
      if road_sign_concept['is_sub_sign']
        @graph << if scraped_sign['is_sub_sign']
                    RDF.Statement(scraped_sign['uri'], LBLOD_MOW['heeftGerelateerdVerkeersbordconcept'],
                                  road_sign_concept['uri'])
                  else
                    RDF.Statement(scraped_sign['uri'], LBLOD_MOW['heeftOnderbordConcept'], road_sign_concept['uri'])
                  end
      elsif scraped_sign['is_sub_sign']
        @graph << RDF.Statement(road_sign_concept['uri'], LBLOD_MOW['heeftOnderbordConcept'], scraped_sign['uri'])
      else
        @graph << RDF.Statement(road_sign_concept['uri'], LBLOD_MOW['heeftGerelateerdVerkeersbordconcept'],
                                scraped_sign['uri'])
      end
    end
  end

  def insert_road_sign_categories
    salt = '4678a9dd-63d8-4876-a419-f2db9285e625'
    subject = RDF::URI(format(SCHEME_BASE_URI, scheme_name: 'Verkeersbordcategorie'))
    uuid = hash(salt + ':' + 'Verkeersbordcategorie')

    @graph << RDF.Statement(subject, RDF.type, SKOS.ConceptScheme)
    @graph << RDF.Statement(subject, MU.uuid, uuid)
    @graph << RDF.Statement(subject, SKOS.prefLabel, 'Verkeersbordcategorie')
    @graph << RDF.Statement(subject, SKOS.note, 'Categorie van een verkeersbord')

    @@road_sign_categorie_name_map.each do |code, name|
      insert_road_sign_category(code, name, subject)
    end
  end

  def insert_road_sign_category(category_code, category_name, conceptscheme_uri)
    salt = 'dd2b2f27-ca5a-4ac1-bd8d-5d67c1cbf9cb'
    uuid = hash(salt + ':' + category_name)
    subject = RDF::URI(format(CONCEPT_BASE_URI, scheme_name: 'Verkeersbordcatergorie', id: uuid))
    @@road_sign_categorie_uri_map[category_code] = subject

    @graph << RDF.Statement(subject, RDF.type, MOB.Verkeersbordcategorie)
    @graph << RDF.Statement(subject, SKOS.prefLabel, category_name)
    @graph << RDF.Statement(subject, MU.uuid, uuid)
    @graph << RDF.Statement(subject, SKOS.topConceptOf, conceptscheme_uri)
    @graph << RDF.Statement(subject, SKOS.inScheme, conceptscheme_uri)
  end

  def insert_verkeersbordconcept_status_codes
    mapping = {}

    salt = 'c4596f16-534b-4b81-af8e-b19919039be7'
    subject = RDF::URI(format(SCHEME_BASE_URI, scheme_name: 'VerkeersbordconceptStatusCode'))
    uuid = hash(salt + ':' + 'VerkeersbordconceptStatusCode')

    @graph << RDF.Statement(subject, RDF.type, SKOS.ConceptScheme)
    @graph << RDF.Statement(subject, MU.uuid, uuid)
    @graph << RDF.Statement(subject, SKOS.prefLabel, 'VerkeersbordconceptStatusCode')
    @graph << RDF.Statement(subject, SKOS.scopeNote, 'Duidt of het verkeersbordconcept nog gebruikt wordt.')

    %w[stabiel onstabiel afgeschaft].each do |row|
      insert_verkeersbordconcept_status_code(mapping, row, subject)
    end
    mapping
  end

  def insert_verkeersbordconcept_status_code(mapping, label, conceptscheme_uri)
    salt = '6f497aa4-9d9d-4c5e-be71-cad0a65417fe'
    uuid = hash(salt + ':' + label)
    subject = RDF::URI(format(CONCEPT_BASE_URI, scheme_name: 'VerkeersbordconceptStatusCode', id: uuid))

    @graph << RDF.Statement(subject, RDF.type, LBLOD_MOW.VerkeersbordconceptStatusCode)
    @graph << RDF.Statement(subject, SKOS.prefLabel, label)
    @graph << RDF.Statement(subject, MU.uuid, uuid)
    @graph << RDF.Statement(subject, SKOS.topConceptOf, conceptscheme_uri)
    @graph << RDF.Statement(subject, SKOS.inScheme, conceptscheme_uri)

    mapping[label] = subject
  end

  def insert_verkeersbordconcept_status(code_uri, code)
    salt = 'af8c3801-ab28-4233-8878-761068bd4807'
    uuid = hash(salt + ':' + code)
    subject = RDF::URI(format(MOW_BASE_URI, resource: 'VerkeersbordconceptStatus', id: uuid))

    @graph << RDF.Statement(subject, RDF.type, MOB.VerkeersbordconceptStatus)
    @graph << RDF.Statement(subject, MOB['Verkeersbordconceptstatus.status'], code_uri)
    @graph << RDF.Statement(subject, MU.uuid, uuid)

    subject
  end

  def hash(str)
    Digest::SHA256.hexdigest str
  end
end

internal_harvester = Road_Sign_ConceptHarvester.new('input/mowdb/detailoverzicht interne borden.csv',
                                                    'output/road_sign_concepts_internal.ttl')
internal_harvester.harvest

be_harvester = Road_Sign_ConceptHarvester.new('input/mowdb/detailoverzicht politieborden.csv',
                                              'output/road_sign_concepts_be.ttl')
be_harvester.harvest
