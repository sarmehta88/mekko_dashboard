library(shiny)
library(shinydashboard)
library(shinyjs)
library(shinyBS)
library(DT)

#--------------------------------------------------------------------------------------
# UI Header
header <- dashboardHeader(
  #theme = "bootswatch-spacelab.css" 
  title = "Wallet Visualization"
)


#--------------------------------------------------------------------------------------
# UI Side Bar
sidebar <- dashboardSidebar(
  
  useShinyjs(),

  sidebarMenu(id = "sidebarmenu",
            
    
    menuItem("Wallet Mekko", icon = icon("th"), tabName = "mekko") , 
       
        
    # add dropdowns for the selection of Sales Hierarchies
    conditionalPanel("input.sidebarmenu === 'mekko'", 
                   radioButtons("pview", "Views",
                                c("Sales Hierarchy" = "Sales Hierarchy","Verticals" = "Verticals","SAV Segments" ="SAV Segments", 
                                  "Partners" ="Partners",
                                  "Technologies" = "Technologies")), 
                   
                   radioButtons("pview2", "", choices =
                                  c("Technologies" = "Technologies", "Verticals" = "Verticals","SAV Segments" ="SAV Segments")),
                   
                   radioButtons("psortby","Sort by",inline = TRUE, 
                                c("Wallet" = "wallet","Incremental BIC" ="bic")), 
                   
                   tags$label("Filters", style = "padding-left:12px;"),
                   uiOutput("pfy_dropdown"),
                   uiOutput("pbeid_partnername"), 
                   div(uiOutput('pdropdown') , style =  "padding-bottom:12px;font-family:helvetica;"), # this is for the Select Partner or SrepName or SAV Group
                   div(uiOutput('remove_unk_checkbox') , style =  "padding-bottom:12px;font-family:helvetica;"), # show this checkbox to remove Unknown values ie. BE-GEO-ID =0 in PartnerNames Level
                   
                   div(bsCollapse(id = "collapseFilters",
                          bsCollapsePanel(title = div("Click to View More Filters", span(class= "caret", style = "float:right; margin-top:8px;border-width:5px;")
                                                      , style = "color:black; font-family:helvetica;"), style = "default",list(
                                 div(uiOutput("ptechgrps_dropdown"), style= "padding-left:12px; padding-top:12px;font-family:helvetica;"),
                                 div(uiOutput("ptechnologies_dropdown"), style= "padding-left:12px; padding-top:30px;font-family:helvetica;"),
                                 div(uiOutput("psavseg_dropdown"),style= "padding-left:12px; padding-top:28px;font-family:helvetica;"),
                                 div(uiOutput("pvert_dropdown"),style= "padding-left:12px; padding-top:28px;font-family:helvetica;")
                                 
                          )# end list
                          )# end bsCollapsePanel
                   )# end bsCollapse
                   , style = "width:93%; padding-left:12px;"), # end div
                   div(uiOutput("pdownloadData"), style= "padding-left:12px; padding-top:15px;font-family:helvetica;"),
                   div(uiOutput("pdownloadBU_data"), style= "padding-left:12px;padding-top:28px;font-family:helvetica;")
                   
  )# end Conditional Panel               
  ,menuItem("High and Low-end Products", icon = icon("th"), tabName = "csvfile")

                   
  ) # end sidebar menu
) # end side bar

#--------------------------------------------------------------------------------------
## UI Body Content
body <- dashboardBody( 
  tags$head(
    tags$style(
      HTML(
        "html {-ms-content-zooming: none;}"
      ))
  ),
  tags$script('
      // Bind function to the toggle sidebar button
              $(".sidebar-toggle").on("click",function(){
                    console.log("RESIZE")
                    $(window).trigger("resize"); // Trigger resize event
              })'
    ),
  tabItems(
    
    # First tab content
    tabItem(tabName = "mekko", 
        
            div(textOutput("pcurr_level"),style = "diplay:inline; font-size:11px;font-weight:800"), 
            div(uiOutput("pfiltered_sel"), style = "color:blue;  font-size:10px; font-weight:400;"),
            # split the page into the mekko chart and total bar
            splitLayout(cellWidths = c("92%", "8%"),
                          #output the mekko chart
                          div(plotOutput("pmekkograph", dblclick = "pplot1_click", hover = hoverOpts(id ="pplot_hover"),width = "103%", height = "750px"), 
                              style = "overflow:hidden;padding-left:0px; padding-top:10px;"),
                          #output the totals bar
                          div(plotOutput("pbar",height = "726px",hover = hoverOpts(id ="pplot2_hover")),style = "overflow:hidden; padding-top:37px"),
                          # add css format to the chart
                          tags$head(
                            tags$style(
                              HTML(".col-sm-12{padding-left:0px; margin-top: -20px;} .content-wrapper{background-color:white;}")
                            )
                          )
              ), #end split layout
              column(width = 12,
                     # add css format to the hover text and download button
                     tags$head(tags$style("#phover_info{color: blue;
                                          font-size: 14px;
                                          font-weight: 400;
                                          }
                                          #pdownload {font-size:8px;}
                                          #pdownloadBU {font-size:8px;}
                                          .panel-title { font-size:14px; margin-left:-12px; height:12px;margin-top:-8px;}
                                          .shiny-html-output {font-family:helvetica;color:black;}
                                          ")
                                          
                                        
                     ),
                     # on bottom of page, add the high/lowend checkbox, download button, hoverinfo, and notes
                     # uiOutput means the ui element is being dynamically created
                     div( 
                       uiOutput("val_vol_lines"),
                       style="float:right;"
                     ),
                     uiOutput("phover_info"),
                     
                     div(id='pnotes',p("* We use # Sites for Unnamed or Prospects accounts"),
                                     p("We use # BE GEO IDs to calculate Avg Wallet/Partner.
                                        The total # SAV accts cannot be filtered by Partner Name, BEID or Technology Group"),
                         style = "float:left; font-size:10px;")
              ) # end the column at the bottom of the mekko chart

    ) #end first Tab Item
    ,tabItem(tabName = "csvfile",
          fluidRow( column( width= 12,
                            DT::dataTableOutput('pftable'), style= "padding:40px;"
          ))
    )
  ) # tabItems()
) # dashboardBody()


#--------------------------------------------------------------------------------------
ui <- dashboardPage(
  header,
  sidebar,
  body,
  title = "Mekko Shiny App"
)

      



