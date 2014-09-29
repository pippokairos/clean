#!/usr/bin/env ruby

require "yaml"
require "pg"

def main
  config = YAML.load_file("config.yml")
  
  database = config['database']
  field = config['field']
  table = config['table']
  json_file_name = config['json_file_name']
  stock_file_name = config['stock_file_name']
  json_file_name = "#{config['database']}_#{config['table']}_#{config['field']}.js"

  conn = PG.connect(dbname: "#{database}")
  conn.exec("BEGIN")
  rows = conn.exec("SELECT #{field} FROM #{table}")
  
  File.open(json_file_name, "a") { |out| 
    out.truncate(0) # delete file content 
    out.print("{\"#{field}\":")
    rows.each do |row|
      out.print("#{row.values[0]},\n")
    end
    out.print("}")
  } 
  
  puts "#{rows.count} rows written to #{json_file_name}."
  
  table_vals = conn.exec("SELECT * FROM #{table}")
  File.open(stock_file_name, "a") { |out|
    out.print("#{table_vals.values[0]}")
  }
  puts "#{rows.cmd_tuples()} rows copied to #{stock_file_name}"
  
  conn.exec("DELETE FROM #{table}")
  puts "#{rows.cmd_tuples()} rows deleted from the table #{table}"
    
  conn.exec("ROLLBACK")
end

main()
