function start_find_comments(sub){
    console.log(sub);
    $.ajax({
        type:"POST",
        url:"/comment_search/start/"+sub,
        success:function(x){
             console.log(x);
             $("#"+sub+"-st").text(x.global + " ["+x.mutex.pid+"]");
        }
    })
}

function refresh_searcher_state(sub){
        console.log(sub);
    $.ajax({
        type:"POST",
        url:"/comment_search/reset_state/"+sub,
        success:function(x){
             console.log(x);
             $("#"+sub+"-st").text("Состояние поиска комментариев сброшено");
        }
    })
}


function clear_process_log(sub){
    $.ajax({
        type:"POST",
        url:"/comment_search/clear_process_log/"+sub,
        success:function(x){
             console.log(x);
             $("#"+sub+"-st").text("Лог процесса очищен");
        }
    });
}