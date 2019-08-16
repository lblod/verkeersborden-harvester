require 'linkeddata'
require 'pry-byebug'
require 'digest/md5'
require 'mimemagic'
require 'fileutils'

class VerkeersbordenHarvester
  # TODO: clean up vocab
  ORG = RDF::Vocab::ORG
  FOAF = RDF::Vocab::FOAF
  SKOS = RDF::Vocab::SKOS
  DC = RDF::Vocab::DC
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  REGORG = RDF::Vocabulary.new("https://www.w3.org/ns/regorg#")
  MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
  NFO = RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#")
  NIE =  RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/01/19/nie#")
  DBPEDIA = RDF::Vocabulary.new("http://dbpedia.org/ontology/")
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  SW = RDF::Vocabulary.new('http://www.w3.org/2003/06/sw-vocab-status/ns#')

  MOB = RDF::Vocabulary.new("https://data.vlaanderen.be/ns/mobiliteit#")
  MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
  BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
  EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")

  BASE_URI = 'http://data.vlaanderen/ns/mobiliteit/%{resource}/%{id}'
  DATA_GIFT = 'http://mobiliteit.vo.data.gift/Verkeersbordconcept/afbeelding/%{id}'
  CONCEPT_BASE_URI = 'http://data.vlaanderen.be/id/concept/%{scheme_name}/%{id}'
  SCHEME_BASE_URI = 'http://data.vlaanderen.be/id/conceptscheme/%{scheme_name}'

  def initialize(input_folder, file_name,  output_folder)
    @graph = RDF::Graph.new
    @graph_code_list = RDF::Graph.new
    @skos_graph = RDF::Graph.new
    @input = File.join(input_folder, file_name)
    @input_folder = input_folder
    @output_folder = output_folder
    @code_list_output_folder = File.join(output_folder, 'codelists')
    @output_files = File.join(output_folder, 'files')
    FileUtils::mkdir_p @code_list_output_folder
    FileUtils::mkdir_p @output_files
  end

  def harvest()
    data = load_table()
    categorieen = load_verkeersbordcategorieen(data)
    categorieen_map = insert_verkeersbordcategorieen(categorieen)
    status_codes_map = insert_verkeersbordconcept_status_codes()
    status_map = insert_verkeersbordconcept_statussen(status_codes_map)
    afbeeldingen_map = insert_verkeersbordafbeeldingen(data)
    insert_verkeersbordconcepten(status_map, categorieen_map, afbeeldingen_map, data)
    write_graph_to_ttl(@output_folder, 'verkeersborden', @graph)
    write_graph_to_ttl(@code_list_output_folder, 'verkeersborden_concept_schemes', @graph_code_list)
  end

  def load_table()
    input = @input
    content = File.read(input);
    document = Nokogiri::HTML(content, nil, 'utf-8')
    table = document.at('table')
    headers = table.search('tr')[0].search('td').map { |h| h.content}
    content = table.search('tr')[1..-1]
    table_data = []

    content.each do |row|
      tds = row.search('td')
      row = {}
      headers.each_with_index do |h, index|
        if(index == 3)
          row[h] = File.join(@input_folder, tds[index].xpath('img/@src')[0].value[2..-1])
        else
          row[h] = tds[index] ? tds[index].content : ''
        end
      end
      table_data << row
    end
    table_data
  end

  def load_verkeersbordcategorieen(data)
     data.map{ |d| d['TypeBord']}.uniq
  end

  def insert_verkeersbordconcept_status_codes()
    mapping = {}
    ['stabiel', 'onstabiel', 'afgeschaft'].each do |row|
      insert_verkeersbordconcept_status_code(mapping, row)
    end
    mapping
  end

  def insert_verkeersbordconcept_status_code(mapping, label)
    salt = '6f497aa4-9d9d-4c5e-be71-cad0a65417fe'
    uuid = hash(salt + ":" + label)
    subject = RDF::URI(BASE_URI % {:resource => 'VerkeersbordconceptstatusCode', :id => uuid})

    @graph << RDF.Statement(subject, RDF.type, MOB.VerkeersbordconceptstatusCode)
    @graph << RDF.Statement(subject, SKOS.prefLabel, label)
    @graph << RDF.Statement(subject, MU.uuid, uuid)

    @graph_code_list << RDF.Statement(subject, RDF.type, MOB.VerkeersbordconceptstatusCode)
    @graph_code_list << RDF.Statement(subject, SKOS.prefLabel, label)

    mapping[label] = subject
  end

  def insert_verkeersbordconcept_statussen(codes_map)
    mapping = {}
    ['stabiel', 'onstabiel', 'afgeschaft'].each do |row|
      insert_verkeersbordconcept_status(mapping, codes_map[row], row)
    end
    mapping
  end

  def insert_verkeersbordconcept_status(mapping, code_uri, label)
    salt = 'af8c3801-ab28-4233-8878-761068bd4807'
    uuid = hash(salt + ":" + label)
    subject = RDF::URI(BASE_URI % {:resource => 'Verkeersbordconceptstatus', :id => uuid})

    @graph << RDF.Statement(subject, RDF.type, MOB.Verkeersbordconceptstatus)
    @graph << RDF.Statement(subject, MOB['Verkeersbordconceptstatus.status'], code_uri)
    @graph << RDF.Statement(subject, MU.uuid, uuid)

    mapping[label] = code_uri
  end

  def insert_verkeersbordcategorieen(categorieen)
    mapping = {}
    categorieen.each do |c|
      insert_verkeersbordcategorie(mapping, c)
    end
    mapping
  end

  def insert_verkeersbordcategorie(mapping, voorkeursnaam)
    salt = "dd2b2f27-ca5a-4ac1-bd8d-5d67c1cbf9cb"
    uuid = hash(salt + ":" + voorkeursnaam)
    subject = RDF::URI(BASE_URI % {:resource => 'Verkeersbordcatergorie', :id => uuid})
    @graph << RDF.Statement(subject, RDF.type, MOB.Vekeersbordcategorie)
    @graph << RDF.Statement(subject, SKOS.prefLabel, voorkeursnaam)
    @graph << RDF.Statement(subject, MU.uuid, uuid)

    @graph_code_list << RDF.Statement(subject, RDF.type, MOB.Vekeersbordcategorie)
    @graph_code_list << RDF.Statement(subject, SKOS.prefLabel, voorkeursnaam)

    mapping[voorkeursnaam] = subject
    mapping
  end

  def insert_verkeersbordafbeeldingen(rows)
    mapping = {}
    rows.each do |row|
      insert_verkeersbordafbeelding(mapping, row['Benaming'], row['Icoon'])
    end
    mapping
  end

  def insert_verkeersbordafbeelding(mapping, verkeersbord_code, url)
    if(!File.file?(url))
      p "Warning: file %{url} not found" % {:url => url}
      return mapping
    end
    #logical file
    salt = 'e98ab6c5-e74a-434f-a11f-4cdb7552785c'
    uuid = hash(salt + ':' + url)
    subject = RDF::URI(DATA_GIFT % {:id => uuid})
    date_now = Time.now.utc.iso8601
    file = File.open(url)
    mime = MimeMagic.by_magic(file).type
    file_name = File.basename(file)
    file_ext = File.extname(file)[1..-1]

    @graph << RDF.Statement(subject, RDF.type, NFO.FileDataObject)
    @graph << RDF.Statement(subject, DC.created, RDF::Literal.new(date_now, datatype: RDF::XSD.datetime))
    @graph << RDF.Statement(subject, DC.modified, RDF::Literal.new(date_now, datatype: RDF::XSD.datetime))
    @graph << RDF.Statement(subject, MU.uuid, uuid)
    @graph << RDF.Statement(subject, DC.format, mime)
    @graph << RDF.Statement(subject, NFO.fileName, file_name)
    @graph << RDF.Statement(subject, NFO.fileSize, file.size)
    @graph << RDF.Statement(subject, DBPEDIA.fileExtension, file_ext)

    #physical file
    salt = '8e462ad2-1396-4cfa-8d58-8ab6317e363e'
    uuid = hash(salt + ':' + url)
    file_name = uuid + '.' + file_ext
    logical_file = subject
    subject = RDF::URI('share://%{file_name}' % {:file_name => file_name})

    @graph << RDF.Statement(subject, RDF.type, NFO.FileDataObject)
    @graph << RDF.Statement(subject, DC.created, RDF::Literal.new(date_now, datatype: RDF::XSD.datetime))
    @graph << RDF.Statement(subject, DC.modified, RDF::Literal.new(date_now, datatype: RDF::XSD.datetime))
    @graph << RDF.Statement(subject, MU.uuid, uuid)
    @graph << RDF.Statement(subject, DC.format, mime)
    @graph << RDF.Statement(subject, NFO.fileName, file_name)
    @graph << RDF.Statement(subject, NFO.fileSize, file.size)
    @graph << RDF.Statement(subject, DBPEDIA.fileExtension, file_ext)
    @graph << RDF.Statement(subject, NIE.dataSource, logical_file)

    #move file
    FileUtils.cp(url, File.join(@output_files, file_name))

    mapping[verkeersbord_code] = logical_file
    mapping
  end

  def insert_verkeersbordconcepten(status_map, categorieen_map, afbeeldingen_map, rows)
    mapping = {}
    rows.each do |row|
      insert_verkeersbordconcept(
        mapping, status_map['stabiel'], afbeeldingen_map[row['Benaming']], row['Betekenis'], categorieen_map[row['TypeBord']], row['Benaming'] )
    end
    mapping
  end

  def insert_verkeersbordconcept(mapping, status_uri, afbeelding_uri, betekenis, classificatie_uri, verkeersbord_code)
    salt = '2864a54c-ad29-49ae-8e38-0f2f790f9f46'
    uuid = hash(salt + ":" + verkeersbord_code)
    subject = RDF::URI(BASE_URI % {:resource => 'Verkeersbordconcept', :id => uuid})

    @graph << RDF.Statement(subject, RDF.type, MOB.Verkeersbordconcept)
    @graph << RDF.Statement(subject, MU.uuid, uuid)
    if(afbeelding_uri)
      @graph << RDF.Statement(subject, MOB.grafischeWeergave, afbeelding_uri)
    end
    @graph << RDF.Statement(subject, SKOS.scopeNote, betekenis)
    @graph << RDF.Statement(subject, ORG.classification, classificatie_uri)
    @graph << RDF.Statement(subject, SW.term_status, status_uri)
    @graph << RDF.Statement(subject, SKOS.prefLabel, verkeersbord_code)

    @graph_code_list << RDF.Statement(subject, RDF.type, MOB.Verkeersbordconcept)
    @graph_code_list << RDF.Statement(subject, SKOS.prefLabel, verkeersbord_code)
    @graph_code_list << RDF.Statement(subject, SKOS.scopeNote, betekenis)

    mapping[verkeersbord_code] = subject
  end

  def hash(str)
    return Digest::SHA256.hexdigest str
  end

  def write_graph_to_ttl(folder, file, graph)
    file_path = File.join(folder, file + '.ttl')
    RDF::Writer.open(file_path) { |writer| writer << graph }
  end

end


harvester = VerkeersbordenHarvester.new('input/20180326-20190813', 'LijstVerkeersSignalisatie.html', './output')
harvester.harvest()
