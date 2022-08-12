function submit_form(evt) {
  const form = evt.target;
  const div = document.getElementById('rendered');
  const style = window.getComputedStyle(div);
  const width_in_ems = Math.floor(parseFloat(style['width']) / parseFloat(style['font-size']));
  const input = document.createElement('input');
  input.setAttribute('type', 'hidden');
  input.setAttribute('name', 'width');
  input.setAttribute('value', width_in_ems);
  form.appendChild(input);
}

function add_tooltip(el, txt) {
  const tt = document.createElement("span")
  tt.classList.add("tooltip")
  const text = document.createTextNode(txt)
  tt.appendChild(text)
  el.appendChild(tt)
  return tt;
}

function remove_tooltips(el) {
  var to_remove = [];
  for(e of el.children) {
    if(e.classList.contains("tooltip")) {
      to_remove.push(e)
    }
  }
  for(e of to_remove) {
    el.removeChild(e)
  }
}

function table_of(el) {
  return window.getComputedStyle(el).getPropertyValue("--table-label");
}

function enter_table_alias_def(evt) {
  const el = evt.currentTarget;
  const current_table = el.dataset.table;
  const user_alias = el.dataset.username;

  for(const e of document.querySelectorAll(".column-ref[data-table=\"" + current_table + "\"]")) {
    e.classList.add("other_reference");
  }
  el.classList.add("self_reference");

  if(user_alias) {
    add_tooltip(el, "user alias: " + user_alias)
  }
}

function leave_table_alias_def(evt) {
  const el = evt.currentTarget;
  const current_table = el.dataset.table;

  for(const e of document.querySelectorAll(".column-ref[data-table=\"" + current_table + "\"]")) {
    e.classList.remove("other_reference");
  }
  el.classList.remove("self_reference");

  remove_tooltips(el);
}

function enter_column_alias_def(evt) {
  const el = evt.currentTarget;
  const current_table = table_of(el);
  if(!current_table) return;

  const current_column = el.dataset.label;
  const name = el.dataset.name;

  for(const e of document.querySelectorAll(".column-ref[data-table=\"" + current_table + "\"][data-column=\"" + current_column + "\"]")) {
    e.classList.add("other_reference");
  }
  el.classList.add("self_reference");

  if(name) {
    add_tooltip(el, "user name: " + name);
  }
}

function leave_column_alias_def(evt) {
  const el = evt.currentTarget;
  const current_table = table_of(el);
  if(!current_table) return;

  const current_column = el.dataset.label;

  for(const e of document.querySelectorAll(".column-ref[data-table=\"" + current_table + "\"][data-column=\"" + current_column + "\"]")) {
    e.classList.remove("other_reference");
  }
  el.classList.remove("self_reference");

  remove_tooltips(el);
}

function enter_select_list_def(evt) {
  const el = evt.currentTarget;
  const current_table = table_of(el);
  if(!current_table) return;
  for(const e of document.querySelectorAll(".table-def[data-label=\"" + current_table + "\"] .select-list-ref[data-idx=\"" + el.dataset.idx + "\"]")) {
    if(table_of(e) === current_table) {
      e.classList.add("other_reference");
    }
  }
  el.classList.add("self_reference");
}

function leave_select_list_def(evt) {
  const el = evt.currentTarget;
  const current_table = table_of(el);
  if(!current_table) return;
  for(const e of document.querySelectorAll(".table-def[data-label=\"" + current_table + "\"] .select-list-ref[data-idx=\"" + el.dataset.idx + "\"]")) {
    if(table_of(e) === current_table) {
      e.classList.remove("other_reference");
    }
  }
  el.classList.remove("self_reference");
}

function enter_select_list_ref(evt) {
  const el = evt.currentTarget;
  const current_table = table_of(el);
  if(!current_table) return;
  for(const e of document.querySelectorAll(".table-def[data-label=\"" + current_table + "\"] .select-list-def[data-idx=\"" + el.dataset.idx + "\"]")) {
    if(table_of(e) === current_table) {
      e.classList.add("defining_reference");
    }
  }
  for(const e of document.querySelectorAll(".table-def[data-label=\"" + current_table + "\"] .select-list-ref[data-idx=\"" + el.dataset.idx + "\"]")) {
    if(table_of(e) === current_table) {
      e.classList.add("other_reference");
    }
  }
  el.classList.add("self_reference");
}

function leave_select_list_ref(evt) {
  const el = evt.currentTarget;
  const current_table = table_of(el);
  if(!current_table) return;
  for(const e of document.querySelectorAll(".table-def[data-label=\"" + current_table + "\"] .select-list-def[data-idx=\"" + el.dataset.idx + "\"]")) {
    if(table_of(e) === current_table) {
      e.classList.remove("defining_reference");
    }
  }
  for(const e of document.querySelectorAll(".table-def[data-label=\"" + current_table + "\"] .select-list-ref[data-idx=\"" + el.dataset.idx + "\"]")) {
    if(table_of(e) === current_table) {
      e.classList.remove("other_reference");
    }
  }
  el.classList.remove("self_reference");
}

function enter_column_ref(evt) {
  const el = evt.currentTarget;
  const current_table = el.dataset.table;
  const current_column = el.dataset.column;

  // definition
  var count = 0;
  for(const e of document.querySelectorAll(".table-def[data-label=\""+current_table+"\"] .column-alias-def[data-label=\""+current_column + "\"]")) {
    if(table_of(e) === current_table) {
      count += 1;
      e.classList.add("defining_reference")
    }
  }
  if(count === 0) {
    for(const e of document.querySelectorAll(".table-alias-def[data-table=\""+current_table+"\"]")) {
      e.classList.add("defining_reference")
    }
  }

  // other references
  for(const e of document.querySelectorAll(".column-ref[data-table=\""+el.dataset.table+"\"][data-column=\""+el.dataset.column+"\"]")) {
    e.classList.add("other_reference")
  }

  el.classList.add("self_reference")
}

function leave_column_ref(evt) {
  const el = evt.currentTarget;
  const current_table = el.dataset.table;
  const current_column = el.dataset.column;

  // definition
  var count = 0;
  for(const e of document.querySelectorAll(".table-def[data-label=\""+current_table+"\"] .column-alias-def[data-label=\""+current_column + "\"]")) {
    if(table_of(e) === current_table) {
      count += 1;
      e.classList.remove("defining_reference")
    }
  }
  if(count === 0) {
    for(const e of document.querySelectorAll(".table-alias-def[data-table=\""+current_table+"\"]")) {
      e.classList.remove("defining_reference")
    }
  }

  // other references
  for(const e of document.querySelectorAll(".column-ref[data-table=\""+el.dataset.table+"\"][data-column=\""+el.dataset.column+"\"]")) {
    e.classList.remove("other_reference")
  }

  el.classList.remove("self_reference")
}

var current_tt = undefined;
function enter_typed(evt) {
  let el = evt.currentTarget;
  if(!el.querySelector(".tooltip")) {
    if(current_tt) {
      current_tt.parentNode.classList.remove("active");
      current_tt.parentNode.removeChild(current_tt);
    }
    current_tt = add_tooltip(el, "type: " + el.dataset.type);
    el.classList.add("active");
  }
}

function leave_typed(evt) {
  const el = evt.currentTarget;
  remove_tooltips(el);
  current_tt = undefined;
  el.classList.remove("active");
}

function init() {
  for(const e of document.querySelectorAll("div.soql .table-alias-def")) {
    e.addEventListener('mouseover', enter_table_alias_def);
    e.addEventListener('mouseleave', leave_table_alias_def);
  }
  for(const e of document.querySelectorAll("div.soql .column-alias-def")) {
    e.addEventListener('mouseover', enter_column_alias_def);
    e.addEventListener('mouseleave', leave_column_alias_def);
  }
  for(const e of document.querySelectorAll("div.soql .select-list-def")) {
    e.addEventListener('mouseover', enter_select_list_def);
    e.addEventListener('mouseleave', leave_select_list_def);
  }
  for(const e of document.querySelectorAll("div.soql .select-list-ref")) {
    e.addEventListener('mouseover', enter_select_list_ref);
    e.addEventListener('mouseleave', leave_select_list_ref);
  }
  for(const e of document.querySelectorAll("div.soql .typed")) {
    e.addEventListener('mouseover', enter_typed);
    e.addEventListener('mouseleave', leave_typed);
  }
  for(const e of document.querySelectorAll("div.soql .column-ref")) {
    e.addEventListener('mouseover', enter_column_ref);
    e.addEventListener('mouseleave', leave_column_ref);
  }

  document.getElementById("form").addEventListener('submit', submit_form);
}

document.addEventListener("DOMContentLoaded", init);
