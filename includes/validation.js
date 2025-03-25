const schema_gcp = 'comunicacao';
const table_name_gcp = 'ultima_comunicacao_assinante';
const database_export = 'gglobo-mkt-ins-hdg-dev';
const schema_export = 'palantir_export';
// const table_name_export = 'export_enriched_user';
// eu sei que é user e não sales, eu coloquei o nome errado na hora de exportar
const table_name_export = 'export_ultima_comunicacao_assinante';
const join_key = ['globo_id', 'id_asset'];
const incremental_condition = "";
const all_columns_count = [//"globo_id",
                            //"id_asset",
                            "tipo_assinante",
                            "sistema",
                            "mais_canais",
                            "addon_disney",
                            "addon_deezer",
                            "dt_assinatura_comeco",
                            "canal_compra",
                            "data_hora_compra",
                            "produto",
                            "tempo_de_base_agrupado",
                            "numero_assinaturas",
                            "numero_dependentes",
                            "gender",
                            "age",
                            "address_state",
                            "first_play",
                            "first_play_subset",
                            "data_first_play",
                            "ultimo_consumo",
                            "ultimo_consumo_subset",
                            "data_ultimo_consumo",
                            "Produto_Contratado",
                            "origem",
                            "telefone",
                            "endereco",
                            "dt_aniversario",
                            "Perfil_Email",
                            "Data_Envio_Ultimo_Email",
                            "Data_Abertura_Ultimo_Email",
                            "Data_Click_Ultimo_Email",
                            "Emails_Enviados_30D",
                            "Emails_Abertos_30D",
                            "Data_Recebimento_Ultimo_Push",
                            "Push_Titulo_Recebido",
                            "Push_Descricao_recebido",
                            "Data_Abertura_Ultimo_Push",
                            "Push_Titulo_Aberto",
                            "Push_Descricao_aberto",
                            "Qtd_Pushs_30D",
                            "Perfil_Uso_Push",
                            "Data_Ultima_Abertura_App",
                            "Abertura_App_Descricao_Push",
                            "Abertura_App_Titulo_Push",
                            "Flag_Recebeu_Email_30D",
                            "Flag_Abriu_Email_30D",
                            "Flag_Recebeu_Push_30D",
                            "Flag_Abriu_App_30D",
                            "Flag_Abriu_Push_30D",
                            "Status_Opt_In"
]; // Keep out the `join_key` column from the list, all columns to count mismatches
const row_check_columns = ["Qtd_Pushs_30D"]; // Columns to check for differences
const JOIN_TYPE = `INNER`

function show_different_values(columns, table1, table2, join_key) {
  var r = `SELECT ${join_key}, `;
  columns.forEach(function(col, i) {
    r += `
        a.${col} AS ${col}_GCP,
        b.${col} AS ${col}_EXPORT,`
  });
  r += `
    FROM ${table1} a ${JOIN_TYPE} JOIN ${table2} b USING (${join_key})
    WHERE `;
  columns.forEach(function(col, i) {
    r += `a.${col} IS DISTINCT FROM b.${col} AND `;
  });
  return r.slice(0, -4);
}

function count_differences_per_column_join(columns, table1, table2, join_key) {
  var r = `SELECT `;
  columns.forEach(function(col, i) {
    r += `SUM(${col}) AS ${col},\n `
  });
  r += `FROM( SELECT ${join_key},\n `;
  columns.forEach(function(col, i) {
    r += `IF(a.${col} IS DISTINCT FROM b.${col}, 1, 0) as ${col},\n `;
  });
  r += `FROM ${table1} a ${JOIN_TYPE} JOIN ${table2} b USING (${join_key}) WHERE `;
  r += join_key.map(function(key) {
    return `b.${key} IS NOT NULL`;
  }).join(` AND `);
  r += `)`;
  return r;
}

function count_total_differences(columns, table1, table2) {
  return `
        SELECT COUNT(*) AS TOTAL FROM 
            (
            (SELECT ${columns}
                FROM ${table1}
                EXCEPT DISTINCT
                SELECT ${columns} from ${table2}
            )
            UNION ALL
            (SELECT ${columns} from ${table2}
                EXCEPT DISTINCT
                SELECT ${columns}
                FROM ${table1}
            )
            )
    `;
}

function validation_columns(columns, join_id, table_gcp, table_export, field_transform, valid_manual_input) {

    validation.columns

    var join_string = ''

    join_id.forEach(function(pk,i)
    {
        join_string += '(gcp_tb.' + pk + ' = palantir_tb.' + pk + ' or (gcp_tb.' + pk + ' IS NULL AND palantir_tb.' + pk + ' IS NULL)) AND '
    });

    join_string = join_string.slice(0,-4)


    var tranform_field_in = ''
    field_transform.forEach(function(col,i)
    {
        tranform_field_in += `\'${col}\',`
    });
    tranform_field_in = tranform_field_in.slice(0, -1)

    var valid_manual = ''
    valid_manual_input.forEach(function(col,i)
    {
        valid_manual += `\'${col}\',`
    });
    valid_manual = valid_manual.slice(0, -1)

    var r = 'WITH total_tb as ('
    columns.forEach(function(col, i) {
        r += `            
            select 
            '${col}' as field,
            SUM(IF((${col}_GCP IS NULL and ${col}_EXPORT IS NULL) OR ${col}_GCP = ${col}_EXPORT,1,0)) as same_value,
            SUM(IF(${col}_GCP is NULL AND ${col}_EXPORT IS NOT NULL ,1,0)) as not_found_dataform_process,
            SUM(IF(${col}_EXPORT is NULL AND ${col}_GCP IS NOT NULL ,1,0)) as not_found_export_palantir,
            SUM(IF(${col}_GCP <> ${col}_EXPORT,1,0)) as dif_values,
            null as diff_values_ex,
            count(0) registros
            from (
            select IF(gcp_tb.${join_id} IS NULL, palantir_tb.${join_id}, gcp_tb.${join_id}) as id, 
            gcp_tb.${col} as ${col}_GCP, palantir_tb.${col} as ${col}_EXPORT,
            IF(gcp_tb.${col} <> palantir_tb.${col} and ROW_NUMBER() OVER( ORDER by IF(gcp_tb.${join_id} IS NULL, palantir_tb.${join_id}, gcp_tb.${join_id})) < 6, STRUCT(CAST(gcp_tb.${col} as STRING) as GCP, CAST(palantir_tb.${col} as STRING) as EXPORT),null) as diff_values_ex
            from ${table_gcp} as gcp_tb
            `+ JOIN_TYPE +`  join ${table_export} as palantir_tb 
            on ` + join_string + `) as join_values
            GROUP BY 1
           
            UNION ALL`
    });
    r = r.slice(0,-9)

    r +=  `)`
    if (valid_manual_input.length > 0 ){
        r += `, valid_manual_info as (`
        valid_manual_input.forEach(function(col, i){
            r += ` select \'${col[0]}\' as field, \'${col[1]}\' as status_manual, \'${col[2]}\' as notes 
                    UNION ALL`

        });
        r = r.slice(0,-9) + `)`

            r += ` select total_tb.field, sum(same_value) as same_value, sum(not_found_dataform_process) as not_found_dataform_process,
            sum(not_found_export_palantir) as not_found_export_palantir, sum(dif_values) as dif_values,
             sum(registros) as registros,
             case 
                when sum(same_value) = sum(registros) or status_manual = \'Validado\' then \'Valid OK\'
                else \'Requer Validacao\' end as status,
            field in (${tranform_field_in}) as field_tranformantion_process, 
            round(sum(same_value) / sum(registros) * 100,2) as perc_equal,
            status_manual as manual_validation_status, notes
            from total_tb
            left join valid_manual_info using(field)
            group by field, status_manual, notes`
    }
    else {
    r += ` select field, sum(same_value) as same_value, sum(not_found_dataform_process) as not_found_dataform_process,
            sum(not_found_export_palantir) as not_found_export_palantir, sum(dif_values) as dif_values,
             sum(registros) as registros,
             case 
                when sum(same_value) = sum(registros) then \'Valid OK\'
                else \'Requer Validacao\' end as status,
            field in (${tranform_field_in}) as field_tranformantion_process, 
            round(sum(same_value) / sum(registros) * 100,2) as perc_equal
            from total_tb
            group by 1`
    }
    return r
}

module.exports = {
  database_export,
  show_different_values,
  count_total_differences,
  count_differences_per_column_join,
  schema_gcp,
  table_name_gcp,
  schema_export,
  table_name_export,
  join_key,
  incremental_condition,
  all_columns_count,
  row_check_columns,
  validation_columns
}
